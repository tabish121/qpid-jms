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

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Test decoding of AMQP Array types
 */
public class ArrayTypeDecoderTest extends CodecTestSupport {

    private final int LARGE_ARRAY_SIZE = 10 * 1024 * 1024;
    private final int SMALL_ARRAY_SIZE = 32;

    @Test
    public void testDecodeSmallBooleanArray() throws IOException {
        doTestDecodeBooleanArrayType(SMALL_ARRAY_SIZE);
    }

    @Test
    public void testDecodeLargeBooleanArray() throws IOException {
        doTestDecodeBooleanArrayType(LARGE_ARRAY_SIZE);
    }

    @Test
    public void testDecodeLargeBooleanArrayUsingProton() throws IOException {
        doTestDecodeBooleanArrayTypeUsingProton(LARGE_ARRAY_SIZE);
    }

    private void doTestDecodeBooleanArrayTypeUsingProton(int size) throws IOException {
        ByteBuf buffer = Unpooled.buffer();

        boolean[] source = new boolean[size];
        for (int i = 0; i < size; ++i) {
            source[i] = i % 2 == 0;
        }

        encodeBooleanArray(buffer, source, true);  // TODO - Use custom encoder

        protonDecoder.setByteBuffer(buffer.nioBuffer());

        Object result = protonDecoder.readObject();
        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertTrue(result.getClass().getComponentType().isPrimitive());

        boolean[] array = (boolean[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
    }

    private void doTestDecodeBooleanArrayType(int size) throws IOException {
        ByteBuf buffer = Unpooled.buffer();

        boolean[] source = new boolean[size];
        for (int i = 0; i < size; ++i) {
            source[i] = i % 2 == 0;
        }

        encodeBooleanArray(buffer, source, true);  // TODO - Use custom encoder

        Object result = decoder.readObject(buffer, decoderState);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertTrue(result.getClass().getComponentType().isPrimitive());

        boolean[] array = (boolean[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
    }
}
