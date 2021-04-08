/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.jms.provider.amqp.message;

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_BYTES_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MAP_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MSG_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_OBJECT_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_STREAM_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_TEXT_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.isContentType;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.qpid.jms.provider.amqp.AmqpConsumer;
import org.apache.qpid.jms.util.ContentTypeSupport;
import org.apache.qpid.jms.util.InvalidContentTypeException;
import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecFactory;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.Encoder;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.messaging.AmqpSequence;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.ApplicationProperties;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.DeliveryAnnotations;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.Header;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.apache.qpid.protonj2.types.messaging.Properties;
import org.apache.qpid.protonj2.types.messaging.Section;

/**
 * AMQP Codec class used to hide the details of encode / decode
 */
public final class AmqpCodec {

    private static class EncoderDecoderContext {
        Decoder decoder = CodecFactory.getDecoder();
        DecoderState decoderState = decoder.newDecoderState();
        Encoder encoder = CodecFactory.getEncoder();
        EncoderState encoderState = encoder.newEncoderState();

        // Store local duplicates from the global cache for thread safety.
        Map<Integer, ProtonBuffer> messageAnnotationsCache = new HashMap<>();
    }

    private static final ThreadLocal<EncoderDecoderContext> TLS_CODEC = new ThreadLocal<EncoderDecoderContext>() {
        @Override
        protected EncoderDecoderContext initialValue() {
            return new EncoderDecoderContext();
        }
    };

    /**
     * Static cache for all cached MessageAnnotation data which is used to populate the
     * duplicate values stored in the TLS Encoder Decoder contexts.  This Map instance must
     * be thread safe as many different producers on different threads can be passing data
     * through this codec and accessing the cache if a TLS duplicate isn't populated yet.
     */
    private static ConcurrentMap<Integer, ProtonBuffer> GLOBAL_ANNOTATIONS_CACHE = new ConcurrentHashMap<>();

    /**
     * @return a Encoder instance.
     */
    public static Encoder getEncoder() {
        return TLS_CODEC.get().encoder;
    }

    /**
     * @return a Decoder instance.
     */
    public static Decoder getDecoder() {
        return TLS_CODEC.get().decoder;
    }

    /**
     * @return a EncoderState instance.
     */
    public static EncoderState getEncoderState() {
        return TLS_CODEC.get().encoderState;
    }

    /**
     * @return a DecoderState instance.
     */
    public static DecoderState getDecoderState() {
        return TLS_CODEC.get().decoderState;
    }

    /**
     * Given an AMQP Section encode it and return the buffer holding the encoded value
     *
     * @param section
     *      the AMQP Section value to encode.
     *
     * @return a buffer holding the encoded bytes of the given AMQP Section object.
     */
    public static ProtonBuffer encode(Section<?> section) {
        if (section == null) {
            return null;
        }

        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Encoder encoder = getEncoder();
        EncoderState encoderState = getEncoderState();

        encoder.writeObject(buffer, encoderState, section);

        return buffer;
    }

    /**
     * Given an encoded AMQP Section, decode the value previously written there.
     *
     * @param encoded
     *      the AMQP Section value to decode.
     *
     * @return a Section object read from its encoded form.
     */
    public static Section<?> decode(ProtonBuffer encoded) {
        if (encoded == null || !encoded.isReadable()) {
            return null;
        }

        Decoder decoder = TLS_CODEC.get().decoder;
        DecoderState decoderState = TLS_CODEC.get().decoderState;

        final Section<?> result;

        int position = encoded.getReadIndex();

        try {
            result = (Section<?>) decoder.readObject(encoded, decoderState);
        } finally {
            encoded.setReadIndex(position);
        }

        return result;
    }

    /**
     * Given a Message instance, encode the Message to the wire level representation
     * of that Message.
     *
     * @param message
     *      the Message that is to be encoded into the wire level representation.
     *
     * @return a buffer containing the wire level representation of the input Message.
     */
    public static ProtonBuffer encodeMessage(AmqpJmsMessageFacade message) {
        EncoderDecoderContext context = TLS_CODEC.get();

        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Encoder encoder = context.encoder;
        EncoderState encoderState = context.encoderState;

        Header header = message.getHeader();
        DeliveryAnnotations deliveryAnnotations = message.getDeliveryAnnotations();
        MessageAnnotations messageAnnotations = message.getMessageAnnotations();
        Properties properties = message.getProperties();
        ApplicationProperties applicationProperties = message.getApplicationProperties();
        Section<?> body = message.getBody();
        Footer footer = message.getFooter();

        if (header != null && !header.isEmpty()) {
            encoder.writeObject(buffer, encoderState, header);
        }
        if (deliveryAnnotations != null) {
            encoder.writeObject(buffer, encoderState, deliveryAnnotations);
        }
        if (messageAnnotations != null) {
            // Ensure annotations contain required message type and destination type data
            AmqpDestinationHelper.setReplyToAnnotationFromDestination(message.getReplyTo(), messageAnnotations);
            AmqpDestinationHelper.setToAnnotationFromDestination(message.getDestination(), messageAnnotations);
            messageAnnotations.getValue().put(AmqpMessageSupport.JMS_MSG_TYPE, message.getJmsMsgType());
            encoder.writeObject(buffer, encoderState, messageAnnotations);
        } else {
            buffer.writeBytes(getCachedMessageAnnotationsBuffer(message, context));
        }
        if (properties != null && !properties.isEmpty()) {
            encoder.writeObject(buffer, encoderState, properties);
        }
        if (applicationProperties != null) {
            encoder.writeObject(buffer, encoderState, applicationProperties);
        }
        if (body != null) {
            encoder.writeObject(buffer, encoderState, body);
        }
        if (footer != null) {
            encoder.writeObject(buffer, encoderState, footer);
        }

        return buffer;
    }

    private static ProtonBuffer getCachedMessageAnnotationsBuffer(AmqpJmsMessageFacade message, EncoderDecoderContext context) {
        byte msgType = message.getJmsMsgType();
        byte toType = AmqpDestinationHelper.toTypeAnnotation(message.getDestination());
        byte replyToType = AmqpDestinationHelper.toTypeAnnotation(message.getReplyTo());

        Integer entryKey = Integer.valueOf((replyToType << 16) | (toType << 8) | msgType);

        ProtonBuffer result = context.messageAnnotationsCache.get(entryKey);
        if (result == null) {
            result = populateMessageAnnotationsCacheEntry(message, entryKey, context);
        }

        return result.setReadIndex(0);
    }

    private static ProtonBuffer populateMessageAnnotationsCacheEntry(AmqpJmsMessageFacade message, Integer entryKey, EncoderDecoderContext context) {
        ProtonBuffer result = GLOBAL_ANNOTATIONS_CACHE.get(entryKey);
        if (result == null) {
            MessageAnnotations messageAnnotations = new MessageAnnotations(new HashMap<>());

            // Sets the Reply To annotation which will likely not be present most of the time so do it first
            // to avoid extra work within the map operations.
            AmqpDestinationHelper.setReplyToAnnotationFromDestination(message.getReplyTo(), messageAnnotations);
            // Sets the To value's destination annotation set and a known JMS destination type likely to always
            // be present but we do allow of edge case of unknown types which won't encode an annotation.
            AmqpDestinationHelper.setToAnnotationFromDestination(message.getDestination(), messageAnnotations);
            // Now store the message type which we know will always be present so do it last to ensure
            // the previous calls don't need to compare anything to this value in the map during add or remove
            messageAnnotations.getValue().put(AmqpMessageSupport.JMS_MSG_TYPE, message.getJmsMsgType());

            // This is the maximum possible encoding size that could appear for all the possible data we
            // store in the cached buffer if the codec was to do the worst possible encode of these types.
            // We could do a custom encoding to make it minimal which would result in a max of 70 bytes.
            result = ProtonByteBufferAllocator.DEFAULT.allocate();

            context.encoder.writeObject(result, context.encoderState, messageAnnotations);

            // Race on populating the global cache could duplicate work but we should avoid keeping
            // both copies around in memory.
            ProtonBuffer previous = GLOBAL_ANNOTATIONS_CACHE.putIfAbsent(entryKey, result);
            if (previous != null) {
                result = previous.duplicate();
            } else {
                result = result.duplicate();
            }
        } else {
            result = result.duplicate();
        }

        context.messageAnnotationsCache.put(entryKey, result);

        return result;
    }

    /**
     * Create a new JmsMessage and underlying JmsMessageFacade that represents the proper
     * message type for the incoming AMQP message.
     *
     * @param consumer
     *        The AmqpConsumer instance that will be linked to the decoded message.
     * @param messageBytes
     *        The the raw bytes that compose the incoming message. (Read-Only)
     *
     * @return a AmqpJmsMessageFacade instance decoded from the message bytes.
     *
     * @throws IOException if an error occurs while creating the message objects.
     */
    public static AmqpJmsMessageFacade decodeMessage(AmqpConsumer consumer, ProtonBuffer messageBytes) throws IOException {

        Decoder decoder = getDecoder();
        DecoderState decoderState = getDecoderState();

        Header header = null;
        DeliveryAnnotations deliveryAnnotations = null;
        MessageAnnotations messageAnnotations = null;
        Properties properties = null;
        ApplicationProperties applicationProperties = null;
        Section<?> body = null;
        Footer footer = null;
        Section<?> section = null;

        while (messageBytes.isReadable()) {
            section = (Section<?>) decoder.readObject(messageBytes, decoderState);

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
                case Data:
                case AmqpSequence:
                case AmqpValue:
                    body = section;
                    break;
                case Footer:
                    footer = (Footer) section;
                    break;
                default:
                    throw new IOException("Unknown Message Section forced decode abort.");
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

    private static AmqpJmsMessageFacade createFromMsgAnnotation(MessageAnnotations messageAnnotations) throws IOException {
        Object annotation = AmqpMessageSupport.getMessageAnnotation(JMS_MSG_TYPE, messageAnnotations);
        if (annotation != null) {
            switch ((byte) annotation) {
                case JMS_MESSAGE:
                    return new AmqpJmsMessageFacade();
                case JMS_BYTES_MESSAGE:
                    return new AmqpJmsBytesMessageFacade();
                case JMS_TEXT_MESSAGE:
                    return new AmqpJmsTextMessageFacade(StandardCharsets.UTF_8);
                case JMS_MAP_MESSAGE:
                    return new AmqpJmsMapMessageFacade();
                case JMS_STREAM_MESSAGE:
                    return new AmqpJmsStreamMessageFacade();
                case JMS_OBJECT_MESSAGE:
                    return new AmqpJmsObjectMessageFacade();
                default:
                    throw new IOException("Invalid JMS Message Type annotation value found in message: " + annotation);
            }
        }

        return null;
    }

    private static AmqpJmsMessageFacade createWithoutAnnotation(Section<?> body, Properties properties) {
        String messageContentType = properties != null ? properties.getContentType() : null;

        if (body == null) {
            if (isContentType(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE, messageContentType)) {
                return new AmqpJmsObjectMessageFacade();
            } else if (isContentType(OCTET_STREAM_CONTENT_TYPE, messageContentType) || isContentType(null, messageContentType)) {
                return new AmqpJmsBytesMessageFacade();
            } else {
                Charset charset = getCharsetForTextualContent(messageContentType);
                if (charset != null) {
                    return new AmqpJmsTextMessageFacade(charset);
                } else {
                    return new AmqpJmsMessageFacade();
                }
            }
        } else if (body instanceof Data) {
            if (isContentType(OCTET_STREAM_CONTENT_TYPE, messageContentType) || isContentType(null, messageContentType)) {
                return new AmqpJmsBytesMessageFacade();
            } else if (isContentType(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE, messageContentType)) {
                return new AmqpJmsObjectMessageFacade();
            } else {
                Charset charset = getCharsetForTextualContent(messageContentType);
                if (charset != null) {
                    return new AmqpJmsTextMessageFacade(charset);
                } else {
                    return new AmqpJmsBytesMessageFacade();
                }
            }
        } else if (body instanceof AmqpValue) {
            Object value = ((AmqpValue<?>) body).getValue();

            if (value == null || value instanceof String) {
                return new AmqpJmsTextMessageFacade(StandardCharsets.UTF_8);
            } else if (value instanceof Binary) {
                return new AmqpJmsBytesMessageFacade();
            } else {
                return new AmqpJmsObjectMessageFacade();
            }
        } else if (body instanceof AmqpSequence) {
            return new AmqpJmsObjectMessageFacade();
        }

        return null;
    }

    private static Charset getCharsetForTextualContent(String messageContentType) {
        if (messageContentType != null) {
            try {
                return ContentTypeSupport.parseContentTypeForTextualCharset(messageContentType);
            } catch (InvalidContentTypeException e) {
            }
        }

        return null;
    }
}
