package org.apache.qpid.jms.provider.amqp.message;

import static org.apache.qpid.jms.provider.amqp.message.AmqpJmsCompressedMessageConstants.COMPRESSED_BYTES_MESSAGE_FORMAT;
import static org.apache.qpid.jms.provider.amqp.message.AmqpJmsCompressedMessageConstants.COMPRESSED_MAP_MESSAGE_FORMAT;
import static org.apache.qpid.jms.provider.amqp.message.AmqpJmsCompressedMessageConstants.COMPRESSED_OBJECT_MESSAGE_FORMAT;
import static org.apache.qpid.jms.provider.amqp.message.AmqpJmsCompressedMessageConstants.COMPRESSED_STREAM_MESSAGE_FORMAT;
import static org.apache.qpid.jms.provider.amqp.message.AmqpJmsCompressedMessageConstants.COMPRESSED_TEXT_MESSAGE_FORMAT;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_BYTES_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MAP_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_OBJECT_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_STREAM_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_TEXT_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.apache.qpid.jms.message.facade.JmsMessageFacade;
import org.apache.qpid.jms.provider.amqp.AmqpConsumer;
import org.apache.qpid.jms.util.IOExceptionSupport;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.EncodingCodes;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * AMQP JMS Message compression codec.
 * <p>
 * Handles both compression and decompression of AMQP mapped JMS messages. The format of an AMQP JMS compressed
 * message is essentially that of the AMQP JMS BytesMessage mapping where the body section is a Data section
 * containing compressed bytes of the original message's {@link Properties} and body {@link Section}'s. The
 * compressed message is sent using a custom message format that identifies the type of messages the compressed
 * version replaced.
 */
public class AmqpJmsCompressedMessageCodec extends AmqpMessageCodec {

    public static final AmqpMessageCodec INSTANCE = new AmqpJmsCompressedMessageCodec();

    /**
     * Type descriptor used when encoding a {@link Data} section
     */
    public static final byte DATA_TYPE_DESCRIPTOR = UnsignedLong.valueOf(0x0000000000000075L).byteValue();

    @Override
    public int getMessageFormat(JmsMessageFacade message) {
        switch(message.getJmsMsgType()) {
            case JMS_OBJECT_MESSAGE:
                return COMPRESSED_OBJECT_MESSAGE_FORMAT;
            case JMS_MAP_MESSAGE:
                return COMPRESSED_MAP_MESSAGE_FORMAT;
            case JMS_BYTES_MESSAGE:
                return COMPRESSED_BYTES_MESSAGE_FORMAT;
            case JMS_STREAM_MESSAGE:
                return COMPRESSED_STREAM_MESSAGE_FORMAT;
            case JMS_TEXT_MESSAGE:
                return COMPRESSED_TEXT_MESSAGE_FORMAT;
            default:
                throw new IllegalArgumentException("Invalid AMQP JMS Type Message returned from source message: " + message.getJmsMsgType());
        }
    }

    @Override
    public ByteBuf encodeMessage(AmqpJmsMessageFacade message) throws IOException {
        final EncoderDecoderContext context = TLS_CODEC.get();
        final AmqpWritableBuffer buffer = new AmqpWritableBuffer();
        final EncoderImpl encoder = context.encoder;

        encoder.setByteBuffer(buffer);

        final Header header = message.getHeader();
        final DeliveryAnnotations deliveryAnnotations = message.getDeliveryAnnotations();
        final MessageAnnotations messageAnnotations = message.getMessageAnnotations();
        final Properties originalProperties = message.getProperties();
        final Properties properties = originalProperties == null ? new Properties() : new Properties(originalProperties);
        final ApplicationProperties applicationProperties = message.getApplicationProperties();
        final Section body = message.getBody();
        final Footer footer = message.getFooter();

        if (header != null) {
            encoder.writeObject(header);
        }
        if (deliveryAnnotations != null) {
            encoder.writeObject(deliveryAnnotations);
        }
        if (messageAnnotations != null) {
            // Ensure annotations contain required message type and destination type data
            AmqpDestinationHelper.setReplyToAnnotationFromDestination(message.getReplyTo(), messageAnnotations);
            AmqpDestinationHelper.setToAnnotationFromDestination(message.getDestination(), messageAnnotations);
            messageAnnotations.getValue().put(AmqpMessageSupport.JMS_MSG_TYPE, JMS_BYTES_MESSAGE);

            encoder.writeObject(messageAnnotations);
        } else {
            buffer.put(getCachedMessageAnnotationsBuffer(message, JMS_BYTES_MESSAGE, context));
        }

        // The compressed ByteMessage should indicate that the payload is a stream of random bytes with no encoding
        // any original content typing is preserved in the compressed payload for later restoration.
        properties.setContentType(OCTET_STREAM_CONTENT_TYPE);
        properties.setContentEncoding(null);

        encoder.writeObject(properties); // Always present in the compressed message

        if (applicationProperties != null) {
            encoder.writeObject(applicationProperties);
        }
        if (body != null) {
            compressMessagePayload(buffer, originalProperties, body);
        }
        if (footer != null) {
            encoder.writeObject(footer);
        }

        encoder.setByteBuffer((WritableBuffer) null);

        return buffer.getBuffer();
    }

    private void compressMessagePayload(AmqpWritableBuffer target, Properties properties, Section body) throws IOException {
        final EncoderImpl encoder = getEncoder();
        final AmqpWritableBuffer bodyEncoding = new AmqpWritableBuffer();
        final Deflater deflator = new Deflater();
        final WritableBuffer originalBuffer = encoder.getBuffer();

        try {
            encoder.setByteBuffer(bodyEncoding);
            // If there was a Properties section in the original message save that into the
            // compressed message body for decode when re-consituting the message.
            if (properties != null) {
                encoder.writeObject(properties);
            }
            encoder.writeObject(body);

            final byte[] scratch = new byte[1024];
            final int sizePosition = target.position();

            target.putLong(0); // Reserved for Data Section

            int compressedBytes = 0;

            // Now compress the encoded body sections which will be written into a data section
            // of a compressed JMS message by the caller.
            deflator.setInput(bodyEncoding.getBuffer().nioBuffer());
            deflator.finish();

            while (!deflator.finished()) {
                final int deflated = deflator.deflate(scratch);

                if (deflated > 0) {
                    target.put(scratch, 0, deflated);
                    compressedBytes += deflated;
                }
            }

            final int endPosition = target.position();

            target.position(sizePosition);
            // Direct encode a Data section to preface the compressed Section that was the
            // original message's body section
            target.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
            target.put(EncodingCodes.SMALLULONG);
            target.put(DATA_TYPE_DESCRIPTOR);
            target.put(EncodingCodes.VBIN32);
            target.putInt(compressedBytes);
            target.position(endPosition); // Restore end state
        } finally {
            encoder.setByteBuffer(originalBuffer);
            deflator.end();
        }
    }

    @Override
    public AmqpJmsMessageFacade decodeMessage(AmqpConsumer consumer, ReadableBuffer messageBytes, int messageFormat) throws IOException {
        switch (messageFormat) {
            case COMPRESSED_BYTES_MESSAGE_FORMAT:
            case COMPRESSED_MAP_MESSAGE_FORMAT:
            case COMPRESSED_OBJECT_MESSAGE_FORMAT:
            case COMPRESSED_STREAM_MESSAGE_FORMAT:
            case COMPRESSED_TEXT_MESSAGE_FORMAT:
                return decodeCompressedMessage(consumer, messageBytes, messageFormat);
            default:
                return AmqpJmsMessageCodec.INSTANCE.decodeMessage(consumer, messageBytes, messageFormat);
        }
    }

    private AmqpJmsMessageFacade decodeCompressedMessage(AmqpConsumer consumer, ReadableBuffer messageBytes, int messageFormat) throws IOException {
        final DecoderImpl decoder = getDecoder();

        Header header = null;
        DeliveryAnnotations deliveryAnnotations = null;
        MessageAnnotations messageAnnotations = null;
        ApplicationProperties applicationProperties = null;
        Data compressedBody = null;
        Footer footer = null;
        Section section = null;

        decoder.setBuffer(messageBytes);

        try {
            while (messageBytes.hasRemaining()) {
                section = (Section) decoder.readObject();

                switch (section.getType()) {
                    case Header:
                        header = (Header) section;
                        break;
                    case DeliveryAnnotations:
                        deliveryAnnotations = (DeliveryAnnotations) section;
                        break;
                    case MessageAnnotations:
                        messageAnnotations = (MessageAnnotations) section;
                        break;
                    case Properties:
                        // Not currently used so not retained here.
                        break;
                    case ApplicationProperties:
                        applicationProperties = (ApplicationProperties) section;
                        break;
                    case Data:
                        compressedBody = (Data) section;
                        break;
                    case Footer:
                        footer = (Footer) section;
                        break;
                    default:
                        throw new IOException("Unknown or invalid Message Section forced decode abort: " + section.getType());
                }
            }
        } finally {
            decoder.setByteBuffer(null);
        }

        final Binary compressedBinary = compressedBody.getValue();
        final Inflater inflater = new Inflater();
        final ByteBuf bodyBytes = Unpooled.buffer(compressedBody.getValue().getLength());
        final byte[] buffer = new byte[1024];

        Section uncompressedBody = null;
        Properties uncompressedProperties = null;

        try {
            if (compressedBinary != null && compressedBinary.getLength() > 0) {
                // We have all the bytes that were sent so we load the inflation object in one go
                // and then move the decoded bytes in chucks to an output buffer as we don't know
                // the full size of the payload until its done. Once finished we can decode the AMQP
                // section that made up the original message.
                inflater.setInput(compressedBinary.getArray(), compressedBinary.getArrayOffset(), compressedBinary.getLength());

                while (!inflater.finished()) {
                    final int bytesDecompressed = inflater.inflate(buffer);

                    bodyBytes.writeBytes(buffer, 0, bytesDecompressed);
                }
            }

            final ReadableBuffer uncompressedBytes = new AmqpReadableBuffer(bodyBytes);

            decoder.setBuffer(uncompressedBytes);

            while (uncompressedBytes.hasRemaining()) {
                section = (Section) decoder.readObject();

                switch (section.getType()) {
                    case Properties:
                        uncompressedProperties = (Properties) section;
                        break;
                    case AmqpValue:
                    case AmqpSequence:
                    case Data:
                        uncompressedBody = section;
                        break;
                    default:
                        throw new IOException("Unknown Message Section forced decode abort.");
                }
            }
        } catch (Exception ex) {
            throw IOExceptionSupport.create(ex);
        } finally {
            decoder.setBuffer(null);
        }

        final AmqpJmsMessageFacade result = createJmsMessageFacade(messageFormat, uncompressedProperties);

        result.setHeader(header);
        result.setDeliveryAnnotations(deliveryAnnotations);
        result.setMessageAnnotations(messageAnnotations);
        result.setApplicationProperties(applicationProperties);
        result.setProperties(uncompressedProperties);
        result.setBody(uncompressedBody);
        result.setFooter(footer);
        result.initialize(consumer);

        return result;
    }

    protected static AmqpJmsMessageFacade createJmsMessageFacade(int messageFormat, Properties properties) throws IOException {
        final Symbol contentType = properties == null ? null : properties.getContentType();

        switch (messageFormat) {
            case COMPRESSED_BYTES_MESSAGE_FORMAT:
                return new AmqpJmsBytesMessageFacade();
            case COMPRESSED_TEXT_MESSAGE_FORMAT:
                return new AmqpJmsTextMessageFacade(getCharsetForTextualContent(contentType, StandardCharsets.UTF_8));
            case COMPRESSED_MAP_MESSAGE_FORMAT:
                return new AmqpJmsMapMessageFacade();
            case COMPRESSED_STREAM_MESSAGE_FORMAT:
                return new AmqpJmsStreamMessageFacade();
            case COMPRESSED_OBJECT_MESSAGE_FORMAT:
                return new AmqpJmsObjectMessageFacade();
            default:
                throw new IOException("Invalid JMS compression Message Format value found in message: " + messageFormat);
        }
    }
}
