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
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ApplicationPropertiesCodecTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024;
    private final int SMALL_SIZE = 32;

    @Test
    public void testApplicationPropertiesWithNullMapProtonToCodec() throws IOException {
        ApplicationProperties properties = new ApplicationProperties(null);

        doTestApplicationPropertiesEncodeAndDecode(properties, true, false);
    }

    @Test
    public void testApplicationPropertiesWithNullMapCodecToProton() throws IOException {
        ApplicationProperties properties = new ApplicationProperties(null);

        doTestApplicationPropertiesEncodeAndDecode(properties, false, true);
    }

    @Test
    public void testApplicationPropertiesWithNullMapCodecToCodec() throws IOException {
        ApplicationProperties properties = new ApplicationProperties(null);

        doTestApplicationPropertiesEncodeAndDecode(properties, false, false);
    }

    @Test
    public void testApplicationPropertiesWithEmptyMapProtonToCodec() throws IOException {
        ApplicationProperties properties = new ApplicationProperties(new LinkedHashMap<>());

        doTestApplicationPropertiesEncodeAndDecode(properties, true, false);
    }

    @Test
    public void testApplicationPropertiesWithEmptyMapCodecToProton() throws IOException {
        ApplicationProperties properties = new ApplicationProperties(new LinkedHashMap<>());

        doTestApplicationPropertiesEncodeAndDecode(properties, false, true);
    }

    @Test
    public void testApplicationPropertiesWithEmptyMapCodecToCodec() throws IOException {
        ApplicationProperties properties = new ApplicationProperties(new LinkedHashMap<>());

        doTestApplicationPropertiesEncodeAndDecode(properties, false, false);
    }

    @Test
    public void testApplicationPropertiesWithNonEmptyMapProtonToCodec() throws IOException {
        Map<String, Object> propertiesMap = new LinkedHashMap<>();
        ApplicationProperties properties = new ApplicationProperties(propertiesMap);

        propertiesMap.put("key-1", "1");
        propertiesMap.put("key-2", "2");
        propertiesMap.put("key-3", "3");
        propertiesMap.put("key-4", "4");
        propertiesMap.put("key-5", "5");
        propertiesMap.put("key-6", "6");
        propertiesMap.put("key-7", "7");
        propertiesMap.put("key-8", "8");

        doTestApplicationPropertiesEncodeAndDecode(properties, true, false);
    }

    @Test
    public void testApplicationPropertiesWithNonEmptyMapCodecToProton() throws IOException {
        Map<String, Object> propertiesMap = new LinkedHashMap<>();
        ApplicationProperties properties = new ApplicationProperties(propertiesMap);

        propertiesMap.put("key-1", "1");
        propertiesMap.put("key-2", "2");
        propertiesMap.put("key-3", "3");
        propertiesMap.put("key-4", "4");
        propertiesMap.put("key-5", "5");
        propertiesMap.put("key-6", "6");
        propertiesMap.put("key-7", "7");
        propertiesMap.put("key-8", "8");

        doTestApplicationPropertiesEncodeAndDecode(properties, false, true);
    }

    @Test
    public void testApplicationPropertiesWithNonEmptyMapCodecToCodec() throws IOException {
        Map<String, Object> propertiesMap = new LinkedHashMap<>();
        ApplicationProperties properties = new ApplicationProperties(propertiesMap);

        propertiesMap.put("key-1", "1");
        propertiesMap.put("key-2", "2");
        propertiesMap.put("key-3", "3");
        propertiesMap.put("key-4", "4");
        propertiesMap.put("key-5", "5");
        propertiesMap.put("key-6", "6");
        propertiesMap.put("key-7", "7");
        propertiesMap.put("key-8", "8");

        doTestApplicationPropertiesEncodeAndDecode(properties, false, false);
    }

    void doTestApplicationPropertiesEncodeAndDecode(ApplicationProperties value, boolean encodeWithProton, boolean decodeWithProton) throws IOException {
        ByteBuf buffer = Unpooled.buffer();

        buffer = encodeObject(buffer, value, encodeWithProton);

        final Object result = decodeObject(buffer, decodeWithProton);

        if (value != null) {
            assertNotNull(result);
            assertTrue(result instanceof ApplicationProperties);
        } else {
            assertNull(result);
        }

        ApplicationProperties decoded = (ApplicationProperties) result;
        assertEquals(value.getValue(), decoded.getValue());
    }

    @Test
    public void testDecodeSmallSeriesOfApplicationProperties() throws IOException {
        doTestDecodeHeaderSeries(SMALL_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfApplicationProperties() throws IOException {
        doTestDecodeHeaderSeries(LARGE_SIZE, false);
    }

    @Test
    public void testDecodeLargeSeriesOfApplicationPropertiesUsingProton() throws IOException {
        doTestDecodeHeaderSeries(LARGE_SIZE, true);
    }

    private void doTestDecodeHeaderSeries(int size, boolean useProton) throws IOException {
        ByteBuf buffer = Unpooled.buffer();

        Map<String, Object> propertiesMap = new LinkedHashMap<>();
        ApplicationProperties properties = new ApplicationProperties(propertiesMap);

        propertiesMap.put("key-1", "1");
        propertiesMap.put("key-2", "2");
        propertiesMap.put("key-3", "3");
        propertiesMap.put("key-4", "4");
        propertiesMap.put("key-5", "5");
        propertiesMap.put("key-6", "6");
        propertiesMap.put("key-7", "7");
        propertiesMap.put("key-8", "8");

        for (int i = 0; i < size; ++i) {
            buffer = encodeObject(buffer, properties, useProton);
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
            assertTrue(result instanceof ApplicationProperties);

            ApplicationProperties decoded = (ApplicationProperties) result;

            assertEquals(8, decoded.getValue().size());
            assertTrue(decoded.getValue().equals(propertiesMap));
        }
    }
}
