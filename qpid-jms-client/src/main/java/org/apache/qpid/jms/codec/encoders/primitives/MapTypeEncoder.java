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
package org.apache.qpid.jms.codec.encoders.primitives;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.qpid.jms.codec.EncoderState;
import org.apache.qpid.jms.codec.EncodingCodes;
import org.apache.qpid.jms.codec.PrimitiveTypeEncoder;
import org.apache.qpid.jms.codec.TypeEncoder;

import java.util.Set;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP Map type values to a byte stream.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class MapTypeEncoder implements PrimitiveTypeEncoder<Map> {

    @Override
    public Class<Map> getTypeClass() {
        return Map.class;
    }

    @Override
    public void writeType(ByteBuf buffer, EncoderState state, Map value) {
        buffer.writeByte(EncodingCodes.MAP32);
        writeValue(buffer, state, value);
    }

    @Override
    public void writeValue(ByteBuf buffer, EncoderState state, Map value) {
        int startIndex = buffer.writerIndex();

        // Reserve space for the size
        buffer.writeInt(0);

        // Record the count of elements which include both key and value in the count.
        buffer.writeInt(value.size() * 2);

        TypeEncoder keyEncoder = null;
        TypeEncoder valueEncoder = null;

        // Write the list elements and then compute total size written.
        Set<Map.Entry> entries = value.entrySet();
        for (Entry entry : entries) {
            Object entryKey = entry.getKey();
            Object entryValue = entry.getValue();

            if (keyEncoder == null || !keyEncoder.getTypeClass().equals(entryKey.getClass())) {
                keyEncoder = state.getEncoder().getTypeEncoder(entry.getKey());
                if (keyEncoder == null) {
                    throw new IllegalArgumentException(
                        "No TypeEncoder exists for requested key type: " + entry.getKey().getClass().getName());
                }
            }

            if (valueEncoder == null || !valueEncoder.getTypeClass().equals(entryValue.getClass())) {
                valueEncoder = state.getEncoder().getTypeEncoder(entry.getValue());
                if (valueEncoder == null) {
                    throw new IllegalArgumentException(
                        "No TypeEncoder exists for requested value type: " + entry.getValue().getClass().getName());
                }
            }

            keyEncoder.writeType(buffer, state, entryKey);
            valueEncoder.writeType(buffer, state, entryValue);
        }

        // Move back and write the size
        int endIndex = buffer.writerIndex();
        buffer.setInt(startIndex, endIndex - startIndex - 4);
    }
}
