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
package org.apache.qpid.jms.integration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Deflater;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsSession;
import org.apache.qpid.jms.message.JmsMessageIDBuilder;
import org.apache.qpid.jms.policy.JmsMessageIDPolicy;
import org.apache.qpid.jms.provider.amqp.message.AmqpCodec;
import org.apache.qpid.jms.provider.amqp.message.AmqpWritableBuffer;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.WritableBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;

/**
 * Base class for JMS Message tests that compress and uncompress message content
 */
public class CompressedMessageTestCase extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    protected JmsMessageIDPolicy messageIDPolicy;

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        messageIDPolicy = new TestJmsMessageIDPolicy();

        super.setUp(testInfo);
    }

    protected Binary encodeAmqpValueContent(Object content, long sendCount) {
        return encodeAmqpValueContent(content, sendCount, null);
    }

    protected Binary encodeAmqpValueContent(Object content, long sendCount, String contentType) {
        return encodeContent(new AmqpValue(content), sendCount, contentType);
    }

    protected Binary encodeSerializedJavaContent(String content, long sendCount, String contentType) throws Exception {
        return encodeContent(new Data(new Binary(getSerializedBytes(content))), sendCount, contentType);
    }

    protected Binary encodeAmqpSequenceContent(List<?> content, long sendCount) {
        return encodeContent(new AmqpSequence(content), sendCount, null);
    }

    protected Binary encodeDataContent(byte[] content, long sendCount) {
        return encodeDataContent(content, sendCount, null);
    }

    protected Binary encodeDataContent(byte[] content, long sendCount, String contentType) {
        return encodeContent(new Data(new Binary(content)), sendCount, contentType);
    }

    protected Binary encodeContent(Section content, long sendCount, String contentType) {
        final EncoderImpl encoder = AmqpCodec.getEncoder();
        final AmqpWritableBuffer buffer = new AmqpWritableBuffer();

        final Properties properties = new Properties();
        properties.setTo(getTestName());
        properties.setMessageId(UnsignedLong.valueOf(sendCount));
        properties.setContentType(Symbol.valueOf(contentType));

        try {
            encoder.setByteBuffer(buffer);
            encoder.writeObject(properties);
            encoder.writeObject(content);
        } finally {
            encoder.setByteBuffer((WritableBuffer) null);
        }

        final ByteBuf encoded = buffer.getBuffer();
        final byte[] encodedBytes = new byte[encoded.readableBytes()];
        final byte[] deflatedBytes = new byte[encoded.readableBytes() + 20]; // can be larger if input is small
        final Deflater deflator = new Deflater();

        try {
            encoded.readBytes(encodedBytes);

            deflator.setInput(encodedBytes);
            deflator.finish();

            final int deflationResult = deflator.deflate(deflatedBytes);

            LOG.debug("Compressed input bytes from {} to {} bytes", encodedBytes.length, deflationResult);

            return new Binary(Arrays.copyOf(deflatedBytes, deflationResult));
        } finally {
            deflator.end();
        }
    }

    private static byte[] getSerializedBytes(Serializable value) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {

            oos.writeObject(value);
            oos.flush();
            oos.close();

            return baos.toByteArray();
        }
    }

    protected static final class TestJmsMessageIDPolicy implements JmsMessageIDPolicy {

        private final AtomicLong nextMessageID = new AtomicLong(1);

        @Override
        public JmsMessageIDPolicy copy() {
            return new TestJmsMessageIDPolicy();
        }

        @Override
        public JmsMessageIDBuilder getMessageIDBuilder(JmsSession session, JmsDestination destination) {
            return new JmsMessageIDBuilder() {

                @Override
                public Object createMessageID(String producerId, long messageSequence) {
                    return UnsignedLong.valueOf(nextMessageID.getAndIncrement());
                }
            };
        }
    }
}
