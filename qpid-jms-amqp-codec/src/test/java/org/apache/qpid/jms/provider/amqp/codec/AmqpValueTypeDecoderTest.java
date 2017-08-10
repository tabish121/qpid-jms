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
package org.apache.qpid.jms.provider.amqp.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.UUID;

import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Test for decoder of the AmqpValue type.
 */
public class AmqpValueTypeDecoderTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024;
    private final int SMALL_SIZE = 32;

    @Test
    public void testDecodeAmqpValueString() throws IOException {
        doTestDecodeAmqpValueSeries(1, false, new AmqpValue("test"));
    }

    @Test
    public void testDecodeAmqpValueNull() throws IOException {
        doTestDecodeAmqpValueSeries(1, false, new AmqpValue(null));
    }

    @Test
    public void testDecodeAmqpValueUUID() throws IOException {
        doTestDecodeAmqpValueSeries(1, false, new AmqpValue(UUID.randomUUID()));
    }

    @Test
    public void testDecodeSmallSeriesOfAmqpValue() throws IOException {
        doTestDecodeAmqpValueSeries(SMALL_SIZE, false, new AmqpValue("test"));
    }

    @Test
    public void testDecodeLargeSeriesOfAmqpValue() throws IOException {
        doTestDecodeAmqpValueSeries(LARGE_SIZE, false, new AmqpValue("test"));
    }

    @Test
    public void testDecodeLargeSeriesOfAmqpValueUsingProton() throws IOException {
        doTestDecodeAmqpValueSeries(LARGE_SIZE, true, new AmqpValue("test"));
    }

    private void doTestDecodeAmqpValueSeries(int size, boolean useProton, AmqpValue value) throws IOException {
        ByteBuf buffer = Unpooled.buffer();

        for (int i = 0; i < size; ++i) {
            buffer = encodeObject(buffer, value, useProton);
        }

        if (useProton) {
            protonDecoder.setByteBuffer(buffer.nioBuffer());
        }

        for (int i = 0; i < size; ++i) {
            final Object result;
            if (useProton) {
                result = protonDecoder.readObject();
            } else {
                result = decoder.readObject(buffer, decoderState);
            }

            assertNotNull(result);
            assertTrue(result instanceof AmqpValue);

            AmqpValue decoded = (AmqpValue) result;

            assertEquals(value.getValue(), decoded.getValue());
        }
    }
}
