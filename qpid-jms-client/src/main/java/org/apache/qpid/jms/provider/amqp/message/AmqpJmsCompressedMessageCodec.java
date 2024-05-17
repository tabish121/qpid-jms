package org.apache.qpid.jms.provider.amqp.message;

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.DEFLATE;

import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

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
import org.apache.qpid.proton.amqp.messaging.Section.SectionType;
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
 * message is essentially that of the AMQP JMS Message types but with a properties section whose content encoding
 * is set to a supported compression type (default is deflate compression). The compressed body is a Data section
 * that wraps the original message section for ease of implementation.
 */
public class AmqpJmsCompressedMessageCodec extends AmqpMessageCodec {

    public static final AmqpMessageCodec INSTANCE = new AmqpJmsCompressedMessageCodec();

    /**
     * Type descriptor used when encoding a {@link Data} section
     */
    public static final byte DATA_TYPE_DESCRIPTOR = UnsignedLong.valueOf(0x0000000000000075L).byteValue();

    @Override
    public ByteBuf encodeMessage(AmqpJmsMessageFacade message) throws IOException {
        final EncoderDecoderContext context = TLS_CODEC.get();
        final AmqpWritableBuffer buffer = new AmqpWritableBuffer();
        final EncoderImpl encoder = context.encoder;

        encoder.setByteBuffer(buffer);

        final Header header = message.getHeader();
        final DeliveryAnnotations deliveryAnnotations = message.getDeliveryAnnotations();
        final MessageAnnotations messageAnnotations = message.getMessageAnnotations();
        final ApplicationProperties applicationProperties = message.getApplicationProperties();
        final Section body = message.getBody();
        final Footer footer = message.getFooter();

        final Properties properties;

        // Only mark content as deflated if there is content.
        if (body != null) {
            properties = message.getProperties() == null ? new Properties() : message.getProperties();
            properties.setContentEncoding(Symbol.valueOf(DEFLATE));
        } else {
            properties = message.getProperties();
        }

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
            messageAnnotations.getValue().put(AmqpMessageSupport.JMS_MSG_TYPE, message.getJmsMsgType());
            encoder.writeObject(messageAnnotations);
        } else {
            buffer.put(getCachedMessageAnnotationsBuffer(message, context));
        }
        if (properties != null) {
            encoder.writeObject(properties);
        }
        if (applicationProperties != null) {
            encoder.writeObject(applicationProperties);
        }
        if (body != null) {
            compressMessagePayload(buffer, body);
        }
        if (footer != null) {
            encoder.writeObject(footer);
        }

        encoder.setByteBuffer((WritableBuffer) null);

        return buffer.getBuffer();
    }

    private void compressMessagePayload(AmqpWritableBuffer target, Section body) throws IOException {
        final EncoderImpl encoder = getEncoder();
        final AmqpWritableBuffer bodyEncoding = new AmqpWritableBuffer();
        final Deflater deflator = new Deflater();
        final WritableBuffer originalBuffer = encoder.getBuffer();

        try {
            encoder.setByteBuffer(bodyEncoding);

            // Compress the body Section and then later write it into a Data section wrapper
            switch (body.getType()) {
                case Data:
                case AmqpSequence:
                case AmqpValue:
                    encoder.writeObject(body);
                    break;
                default:
                    throw new IOException("Unknown Message Section forced compress abort.");
            }

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
            // Direct encode a Data section to preface the compressed binary that was the
            // original message's body value
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
    public AmqpJmsMessageFacade decodeMessage(AmqpConsumer consumer, ReadableBuffer messageBytes) throws IOException {
        final DecoderImpl decoder = getDecoder();

        Header header = null;
        DeliveryAnnotations deliveryAnnotations = null;
        MessageAnnotations messageAnnotations = null;
        Properties properties = null;
        ApplicationProperties applicationProperties = null;
        Section body = null;
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
                        properties = (Properties) section;
                        break;
                    case ApplicationProperties:
                        applicationProperties = (ApplicationProperties) section;
                        break;
                    case AmqpSequence:
                    case AmqpValue:
                    case Data:
                        body = section;
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

        final String contentEncoding;

        if (properties != null && properties.getContentEncoding() != null) {
            contentEncoding = properties.getContentEncoding().toString();
        } else {
            contentEncoding = null;
        }

        if (body != null && body.getType().equals(SectionType.Data) && properties != null && DEFLATE.equals(contentEncoding)) {
            final Binary compressedBinary = ((Data)body).getValue();

            properties.setContentEncoding(null); // Clear encoding value as we have decoded it.

            if (compressedBinary != null && compressedBinary.getLength() > 0) {
                final Inflater inflater = new Inflater();
                final ByteBuf bodyBytes = Unpooled.buffer(compressedBinary.getLength());
                final byte[] buffer = new byte[1024];

                try {
                    // We have all the bytes that were sent so we load the inflation object in one go
                    // and then move the decoded bytes in chucks to an output buffer as we don't know
                    // the full size of the payload until its done. Once finished we can decode the AMQP
                    // section that made up the original message.
                    inflater.setInput(compressedBinary.getArray(), compressedBinary.getArrayOffset(), compressedBinary.getLength());

                    while (!inflater.finished()) {
                        final int bytesDecompressed = inflater.inflate(buffer);

                        bodyBytes.writeBytes(buffer, 0, bytesDecompressed);
                    }

                    final ReadableBuffer uncompressedBytes = new AmqpReadableBuffer(bodyBytes);

                    decoder.setBuffer(uncompressedBytes);

                    while (uncompressedBytes.hasRemaining()) {
                        section = (Section) decoder.readObject();

                        switch (section.getType()) {
                            case AmqpValue:
                            case AmqpSequence:
                            case Data:
                                body = section;
                                break;
                            default:
                                throw new IOException("Unknown decompressed Message Section forced decode abort.");
                        }
                    }
                } catch (Exception ex) {
                    throw IOExceptionSupport.create(ex);
                } finally {
                    decoder.setBuffer(null);
                }
            }
        }

        // First we try the easy way, if the annotation is there we don't have to work hard.
        AmqpJmsMessageFacade result = createFromMsgAnnotation(messageAnnotations);

        if (result == null) {
            // Next, match specific section structures and content types
            result = createWithoutAnnotation(body, properties);
        }

        if (result != null) {
            result.setHeader(header);
            result.setDeliveryAnnotations(deliveryAnnotations);
            result.setMessageAnnotations(messageAnnotations);
            result.setProperties(properties);
            result.setApplicationProperties(applicationProperties);
            result.setBody(body);
            result.setFooter(footer);
            result.initialize(consumer);

            return result;
        }

        throw new IOException("Could not create a JMS message from incoming message");
    }
}
