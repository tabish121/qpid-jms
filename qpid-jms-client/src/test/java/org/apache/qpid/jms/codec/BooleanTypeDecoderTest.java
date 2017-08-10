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
package org.apache.qpid.jms.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Test the BooleanTypeDecoder for correctness
 */
public class BooleanTypeDecoderTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024;
    private final int SMALL_SIZE = 32;

    @Test
    public void testDecodeBooleanTrue() throws Exception {
        ByteBuf buffer = Unpooled.buffer();

        encodeBoolean(buffer, true, false);

        Object result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof Boolean);
        assertTrue(((Boolean) result).booleanValue());
    }

    @Test
    public void testDecodeBooleanFalse() throws Exception {
        ByteBuf buffer = Unpooled.buffer();

        encodeBoolean(buffer, false, false);

        Object result = decoder.readObject(buffer, decoderState);
        assertTrue(result instanceof Boolean);
        assertFalse(((Boolean) result).booleanValue());
    }

    @Test
    public void testDecodeSmallSeriesOfBooleans() throws IOException {
        doTestDecodeBooleanSeries(SMALL_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfBooleans() throws IOException {
        doTestDecodeBooleanSeries(LARGE_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfBooleansUsingProton() throws IOException {
        doTestDecodeBooleanSeries(LARGE_SIZE, true);
    }

    private void doTestDecodeBooleanSeries(int size, boolean useProton) throws IOException {
        ByteBuf buffer = Unpooled.buffer();

        for (int i = 0; i < size; ++i) {
            buffer = encodeBoolean(buffer, i % 2 == 0, useProton);
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
            assertTrue(result instanceof Boolean);

            Boolean boolValue = (Boolean) result;
            assertEquals(i % 2 == 0, boolValue.booleanValue());
        }
    }
}
