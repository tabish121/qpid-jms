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
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.qpid.jms.provider.amqp.AmqpConsumer;
import org.apache.qpid.jms.util.ContentTypeSupport;
import org.apache.qpid.jms.util.InvalidContentTypeException;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.codec.AMQPDefinedTypes;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;

import io.netty.buffer.ByteBuf;

/**
 * Base class used to define the encode and decode process for incoming and outgoing AMQP messages.
 */
public abstract class AmqpMessageCodec {

    /**
     * Encode the source AMQP message into a byte buffer for transmission.
     *
     * @param message
     * 		The message to encode.
     *
     * @return a {@link ByteBuf} that contains the encoded message bytes
     *
     * @throws IOException if an error occurs during the message encode process.
     */
    public abstract ByteBuf encodeMessage(AmqpJmsMessageFacade message) throws IOException;

    /**
     * TODO
     *
     * @param consumer
     * @param messageBytes
     *
     * @return the decoded AMQP message in a JMS message facade wrapper.
     *
     * @throws IOException if an error occurs during the message decode process.
     */
    public abstract AmqpJmsMessageFacade decodeMessage(AmqpConsumer consumer, ReadableBuffer messageBytes) throws IOException;

    /**
     * @return a Encoder instance.
     */
    public static EncoderImpl getEncoder() {
        return AmqpMessageCodec.TLS_CODEC.get().encoder;
    }

    /**
     * @return a Decoder instance.
     */
    public static DecoderImpl getDecoder() {
        return AmqpMessageCodec.TLS_CODEC.get().decoder;
    }

    //----- Internal Implementation details common to multiple Codecs

    /**
     * Static cache for all cached MessageAnnotation data which is used to populate the
     * duplicate values stored in the TLS Encoder Decoder contexts.  This Map instance must
     * be thread safe as many different producers on different threads can be passing data
     * through this codec and accessing the cache if a TLS duplicate isn't populated yet.
     */
    protected static ConcurrentMap<Integer, ReadableBuffer> GLOBAL_ANNOTATIONS_CACHE = new ConcurrentHashMap<>();

    protected static class EncoderDecoderContext {
        final DecoderImpl decoder = new DecoderImpl();
        final EncoderImpl encoder = new EncoderImpl(decoder);
        {
            AMQPDefinedTypes.registerAllTypes(decoder, encoder);
        }

        // Store local duplicates from the global cache for thread safety.
        final Map<Integer, ReadableBuffer> messageAnnotationsCache = new HashMap<>();
    }

    protected static final ThreadLocal<EncoderDecoderContext> TLS_CODEC = new ThreadLocal<EncoderDecoderContext>() {
        @Override
        protected EncoderDecoderContext initialValue() {
            return new EncoderDecoderContext();
        }
    };

    protected static ReadableBuffer getCachedMessageAnnotationsBuffer(AmqpJmsMessageFacade message, EncoderDecoderContext context) {
        return getCachedMessageAnnotationsBuffer(message, message.getJmsMsgType(), context.encoder, context.messageAnnotationsCache);
    }

    protected static ReadableBuffer getCachedMessageAnnotationsBuffer(AmqpJmsMessageFacade message, byte msgType, EncoderDecoderContext context) {
        return getCachedMessageAnnotationsBuffer(message, msgType, context.encoder, context.messageAnnotationsCache);
    }

    protected static ReadableBuffer getCachedMessageAnnotationsBuffer(AmqpJmsMessageFacade message, byte msgType, EncoderImpl encoder, Map<Integer, ReadableBuffer> messageAnnotationsCache) {
        final byte toType = AmqpDestinationHelper.toTypeAnnotation(message.getDestination());
        final byte replyToType = AmqpDestinationHelper.toTypeAnnotation(message.getReplyTo());
        final Integer entryKey = Integer.valueOf((replyToType << 16) | (toType << 8) | msgType);

        ReadableBuffer result = messageAnnotationsCache.get(entryKey);

        if (result == null) {
            result = populateMessageAnnotationsCacheEntry(message, msgType, entryKey, encoder, messageAnnotationsCache);
        }

        return result.rewind();
    }

    protected static ReadableBuffer populateMessageAnnotationsCacheEntry(AmqpJmsMessageFacade message, byte msgType, Integer entryKey, EncoderImpl encoder, Map<Integer, ReadableBuffer> messageAnnotationsCache) {
        ReadableBuffer result = GLOBAL_ANNOTATIONS_CACHE.get(entryKey);

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
            messageAnnotations.getValue().put(AmqpMessageSupport.JMS_MSG_TYPE, msgType);

            // This is the maximum possible encoding size that could appear for all the possible data we
            // store in the cached buffer if the codec was to do the worst possible encode of these types.
            // We could do a custom encoding to make it minimal which would result in a max of 70 bytes.
            final ByteBuffer buffer = ByteBuffer.allocate(124);

            final WritableBuffer oldBuffer = encoder.getBuffer();

            encoder.setByteBuffer(buffer);
            encoder.writeObject(messageAnnotations);
            encoder.setByteBuffer(oldBuffer);

            buffer.flip();

            result = ReadableBuffer.ByteBufferReader.wrap(buffer);

            // Race on populating the global cache could duplicate work but we should avoid keeping
            // both copies around in memory.
            ReadableBuffer previous = GLOBAL_ANNOTATIONS_CACHE.putIfAbsent(entryKey, result);
            if (previous != null) {
                result = previous.duplicate();
            } else {
                result = result.duplicate();
            }
        } else {
            result = result.duplicate();
        }

        messageAnnotationsCache.put(entryKey, result);

        return result;
    }

    protected static AmqpJmsMessageFacade createFromMsgAnnotation(MessageAnnotations messageAnnotations) throws IOException {
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

    protected static AmqpJmsMessageFacade createWithoutAnnotation(Section body, Properties properties) {
        Symbol messageContentType = properties != null ? properties.getContentType() : null;

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
            Object value = ((AmqpValue) body).getValue();

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

    protected static Charset getCharsetForTextualContent(Symbol messageContentType) {
        return getCharsetForTextualContent(messageContentType, null);
    }

    protected static Charset getCharsetForTextualContent(Symbol messageContentType, Charset defaultCharset) {
        if (messageContentType != null) {
            try {
                return ContentTypeSupport.parseContentTypeForTextualCharset(messageContentType.toString());
            } catch (InvalidContentTypeException e) {
            }
        }

        return defaultCharset;
    }
}
