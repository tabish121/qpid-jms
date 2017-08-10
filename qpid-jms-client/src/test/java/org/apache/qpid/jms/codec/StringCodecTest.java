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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class StringCodecTest extends CodecTestSupport {

    private final int LARGE_SIZE = 2048;
    private final int SMALL_SIZE = 32;

    private final String SMALL_STRING_VALUIE = "Small String";
    private final String LARGE_STRING_VALUIE = "Large String: " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog.";

    @Test
    public void testEncodeSmallStringWithProtonDecodeWithCodec() throws IOException {
        doTestEncodeDecode(SMALL_STRING_VALUIE, true, false);
    }

    @Test
    public void testEncodeSmallStringCodecDecodeWithProton() throws IOException {
        doTestEncodeDecode(SMALL_STRING_VALUIE, false, true);
    }

    @Test
    public void testEncodeSmallStringCodecDecodeWithCodec() throws IOException {
        doTestEncodeDecode(SMALL_STRING_VALUIE, false, false);
    }

    @Test
    public void testEncodeLargeStringWithProtonDecodeWithCodec() throws IOException {
        doTestEncodeDecode(LARGE_STRING_VALUIE, true, false);
    }

    @Test
    public void testEncodeLargeStringCodecDecodeWithProton() throws IOException {
        doTestEncodeDecode(LARGE_STRING_VALUIE, false, true);
    }

    @Test
    public void testEncodeLargeStringCodecDecodeWithCodec() throws IOException {
        doTestEncodeDecode(LARGE_STRING_VALUIE, false, false);
    }

    @Test
    public void testEncodeEmptyStringWithProtonDecodeWithCodec() throws IOException {
        doTestEncodeDecode("", true, false);
    }

    @Test
    public void testEncodeEmptyStringCodecDecodeWithProton() throws IOException {
        doTestEncodeDecode("", false, true);
    }

    @Test
    public void testEncodeEmptyStringCodecDecodeWithCodec() throws IOException {
        doTestEncodeDecode("", false, false);
    }

    @Test
    public void testEncodeNullStringWithProtonDecodeWithCodec() throws IOException {
        doTestEncodeDecode(null, true, false);
    }

    @Test
    public void testEncodeNullStringCodecDecodeWithProton() throws IOException {
        doTestEncodeDecode(null, false, true);
    }

    @Test
    public void testEncodeNullStringCodecDecodeWithCodec() throws IOException {
        doTestEncodeDecode(null, false, false);
    }

    private void doTestEncodeDecode(String value, boolean encodeWithProton, boolean decodeWithProton) throws IOException {
        ByteBuf buffer = Unpooled.buffer();

        buffer = encodeString(buffer, value, encodeWithProton);

        final Object result = decodeObject(buffer, decodeWithProton);

        if (value != null) {
            assertNotNull(result);
            assertTrue(result instanceof String);
        } else {
            assertNull(result);
        }
        assertEquals(value, result);
    }

    @Test
    public void testDecodeSmallSeriesOfStrings() throws IOException {
        doTestDecodeStringSeries(SMALL_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfStrings() throws IOException {
        doTestDecodeStringSeries(LARGE_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfStringsUsingProton() throws IOException {
        doTestDecodeStringSeries(LARGE_SIZE, true);
    }

    private void doTestDecodeStringSeries(int size, boolean useProton) throws IOException {
        ByteBuf buffer = Unpooled.buffer();

        for (int i = 0; i < size; ++i) {
            buffer = encodeString(buffer, LARGE_STRING_VALUIE, useProton);
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
            assertTrue(result instanceof String);
            assertEquals(LARGE_STRING_VALUIE, result);
        }
    }
}
