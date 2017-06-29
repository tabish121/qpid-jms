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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.jms.provider.amqp.codec.Decoder;
import org.apache.qpid.jms.provider.amqp.codec.DecoderState;
import org.apache.qpid.jms.provider.amqp.codec.Encoder;
import org.apache.qpid.jms.provider.amqp.codec.EncoderState;
import org.apache.qpid.jms.provider.amqp.codec.decoders.ProtonDecoderFactory;
import org.apache.qpid.jms.provider.amqp.codec.encoders.ProtonEncoderFactory;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Test for decoder of AMQP Properties type.
 */
public class AmqpPropertiesCodecTest {

    private final int LARGE_SIZE = 1024 * 1024;
    private final int SMALL_SIZE = 32;

    protected DecoderState decoderState;
    protected EncoderState encoderState;
    protected Decoder decoder;
    protected Encoder encoder;

    @Before
    public void setUp() {
        decoder = ProtonDecoderFactory.create();
        decoderState = decoder.newDecoderState();

        encoder = ProtonEncoderFactory.create();
        encoderState = encoder.newEncoderState();

        decoder.registerDescribedTypeDecoder(new AmqpPropertiesTypeDecoder());
        encoder.registerTypeEncoder(new AmqpPropertiesTypeEncoder());
    }

    @Test
    public void testDecodeLargeSeriesOfPropertiess() throws IOException {
        doTestDecodePropertiesSeries(LARGE_SIZE);
    }

    @Test
    public void testDecodeSmallSeriesOfPropertiess() throws IOException {
        doTestDecodePropertiesSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfPropertiessUsingProton() throws IOException {
        doTestDecodePropertiesSeries(LARGE_SIZE);
    }

    @Test
    public void testPerformance() throws IOException {
        doTestDecodePropertiesSeries(LARGE_SIZE);

        long startTime = System.nanoTime();

        doTestDecodePropertiesSeries(LARGE_SIZE);

        long totalTime = System.nanoTime() - startTime;

        System.out.println("Total time spent in codec = " + TimeUnit.NANOSECONDS.toMillis(totalTime) + "ms");
    }

    private void doTestDecodePropertiesSeries(int size) throws IOException {
        ByteBuf buffer = Unpooled.buffer();

        AmqpProperties properties = new AmqpProperties();

        Date timeNow = new Date(System.currentTimeMillis());

        properties.setMessageId("ID:Message-1:1:1:0");
        properties.setUserId(new Binary(new byte[1]));
        properties.setTo("queue:work");
        properties.setSubject("help");
        properties.setReplyTo("queue:temp:me");
        properties.setContentEncoding(Symbol.valueOf("text/UTF-8"));
        properties.setContentType(Symbol.valueOf("text"));
        properties.setCorrelationId("correlation-id");
        properties.setAbsoluteExpiryTime(timeNow.getTime());
        properties.setCreationTime(timeNow.getTime());
        properties.setGroupId("group-1");
        properties.setGroupSequence(UnsignedInteger.valueOf(1));
        properties.setReplyToGroupId("group-1");

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(buffer, encoderState, properties);
        }

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject(buffer, decoderState);

            assertNotNull(result);
            assertTrue(result instanceof AmqpProperties);

            AmqpProperties decoded = (AmqpProperties) result;

            assertNotNull(decoded.getAbsoluteExpiryTime());
            assertEquals("ID:Message-1:1:1:0", decoded.getMessageId());
            assertEquals(timeNow.getTime(), decoded.getAbsoluteExpiryTime());
            assertEquals(Symbol.valueOf("text/UTF-8"), decoded.getRawContentEncoding());
            assertEquals(Symbol.valueOf("text"), decoded.getRawContentType());
            assertEquals("correlation-id", decoded.getCorrelationId());
            assertEquals(timeNow.getTime(), decoded.getCreationTime());
            assertEquals("group-1", decoded.getGroupId());
            assertEquals(UnsignedInteger.valueOf(1), decoded.getRawGroupSequence());
            assertEquals("queue:temp:me", decoded.getReplyTo());
            assertEquals("group-1", decoded.getReplyToGroupId());
            assertEquals("help", decoded.getSubject());
            assertEquals("queue:work", decoded.getTo());
            assertTrue(decoded.getUserId() instanceof Binary);
        }
    }
}
