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

import java.io.IOException;

import org.apache.qpid.jms.message.facade.JmsMessageFacade;
import org.apache.qpid.jms.provider.amqp.AmqpConsumer;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;

import io.netty.buffer.ByteBuf;

/**
 * AMQP Codec class used to hide the details of encode / decode
 */
public final class AmqpCodec {

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

    /**
     * Given an AMQP Section encode it and return the buffer holding the encoded value
     *
     * @param section
     *      the AMQP Section value to encode.
     *
     * @return a buffer holding the encoded bytes of the given AMQP Section object.
     */
    public static ByteBuf encode(Section section) {
        if (section == null) {
            return null;
        }

        AmqpWritableBuffer buffer = new AmqpWritableBuffer();

        EncoderImpl encoder = getEncoder();
        encoder.setByteBuffer(buffer);
        encoder.writeObject(section);
        encoder.setByteBuffer((WritableBuffer) null);

        return buffer.getBuffer();
    }

    /**
     * Given an encoded AMQP Section, decode the value previously written there.
     *
     * @param encoded
     *      the AMQP Section value to decode.
     *
     * @return a Section object read from its encoded form.
     */
    public static Section decode(ByteBuf encoded) {
        if (encoded == null || !encoded.isReadable()) {
            return null;
        }

        DecoderImpl decoder = AmqpMessageCodec.TLS_CODEC.get().decoder;
        decoder.setByteBuffer(encoded.nioBuffer());
        Section result = (Section) decoder.readObject();
        decoder.setByteBuffer(null);
        encoded.resetReaderIndex();

        return result;
    }

    /**
     * Given a Message instance, encode the Message to the wire level representation
     * of that Message.
     *
     * @param message
     *      the Message that is to be encoded into the wire level representation.
     * @param canCompress
     * 		If the selection process should include compression in the codec choice.
     *
     * @return a buffer containing the wire level representation of the input Message.
     */
    public static ByteBuf encodeMessage(AmqpJmsMessageFacade message, boolean canCompress) throws IOException {
        if (canCompress && message.hasBody()) {
            return AmqpJmsCompressedMessageCodec.INSTANCE.encodeMessage(message);
        } else {
            return AmqpJmsMessageCodec.INSTANCE.encodeMessage(message);
        }
    }

    /**
     * Select an appropriate message codec for use in encoding the given message based on
     * the message content and or the compression allowed indicator.
     *
     * @param message
     * 		The {@link JmsMessageFacade} instance that will be encoded using the returned codec.
     * @param canCompress
     * 		If the selection process should include compression in the codec choice.
     *
     * @return an {@link AmqpMessageCodec} that should be used for encoding the message.
     */
    public static AmqpMessageCodec selectEncoder(JmsMessageFacade message, boolean canCompress) {
        if (canCompress && message.hasBody()) {
            return AmqpJmsCompressedMessageCodec.INSTANCE;
        } else {
            return AmqpJmsMessageCodec.INSTANCE;
        }
    }

    /**
     * Create a new JmsMessage and underlying JmsMessageFacade that represents the proper
     * message type for the incoming AMQP message.
     *
     * @param consumer
     *        The AmqpConsumer instance that will be linked to the decoded message.
     * @param messageBytes
     *        The the raw bytes that compose the incoming message. (Read-Only)
     * @param messageFormat
     * 		  The AMQP message format that was set on the Transfer that contained the message.
     *
     * @return a AmqpJmsMessageFacade instance decoded from the message bytes.
     *
     * @throws IOException if an error occurs while creating the message objects.
     */
    public static AmqpJmsMessageFacade decodeMessage(AmqpConsumer consumer, ReadableBuffer messageBytes, int messageFormat) throws IOException {
        if (messageFormat == 0) {
            return AmqpJmsMessageCodec.INSTANCE.decodeMessage(consumer, messageBytes, messageFormat);
        } else {
            return AmqpJmsCompressedMessageCodec.INSTANCE.decodeMessage(consumer, messageBytes, messageFormat);
        }
    }
}
