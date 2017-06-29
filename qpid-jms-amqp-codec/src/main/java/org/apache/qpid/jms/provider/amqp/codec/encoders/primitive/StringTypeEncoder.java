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
package org.apache.qpid.jms.provider.amqp.codec.encoders.primitive;

import org.apache.qpid.jms.provider.amqp.codec.EncoderState;
import org.apache.qpid.jms.provider.amqp.codec.EncodingCodes;
import org.apache.qpid.jms.provider.amqp.codec.PrimitiveTypeEncoder;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP String type values to a byte stream.
 */
public class StringTypeEncoder implements PrimitiveTypeEncoder<String> {

    @Override
    public Class<String> getTypeClass() {
        return String.class;
    }

    @Override
    public void writeType(ByteBuf buffer, EncoderState state, String value) {
        write(buffer, state, value, true);
    }

    @Override
    public void writeValue(ByteBuf buffer, EncoderState state, String value) {
        write(buffer, state, value, false);
    }

    private void write(ByteBuf buffer, EncoderState state, String value, boolean writeEncoding) {
        int startIndex = buffer.writerIndex() + 1;

        int fieldWidth = 1;

        // We are pessimistic and assume larger strings will encode
        // at the max 4 bytes per character instead of calculating
        if (value.length() > 64) {
            fieldWidth = 4;
        }

        // Reserve space for the size
        if (fieldWidth == 1) {
            if (writeEncoding) {
                buffer.writeByte(EncodingCodes.STR8);
            }
            buffer.writeByte(0);
        } else {
            if (writeEncoding) {
                buffer.writeByte(EncodingCodes.STR32);
            }
            buffer.writeInt(0);
        }

        // Write the full string value
        writeString(buffer, state, value);

        // Move back and write the size
        int endIndex = buffer.writerIndex();
        if (fieldWidth == 1) {
            buffer.setByte(startIndex, endIndex - startIndex - fieldWidth);
        } else {
            buffer.setInt(startIndex, endIndex - startIndex - fieldWidth);
        }
    }

    private void writeString(ByteBuf buffer, EncoderState state, String value) {
        final int length = value.length();
        int c;

        for (int i = 0; i < length; i++) {
            c = value.charAt(i);
            if ((c & 0xFF80) == 0) {
                /* U+0000..U+007F */
                buffer.writeByte((byte) c);
            } else if ((c & 0xF800) == 0) {
                /* U+0080..U+07FF */
                buffer.writeByte((byte)(0xC0 | ((c >> 6) & 0x1F)));
                buffer.writeByte((byte)(0x80 | (c & 0x3F)));
            } else if ((c & 0xD800) != 0xD800 || (c > 0xDBFF)) {
                /* U+0800..U+FFFF - excluding surrogate pairs */
                buffer.writeByte((byte)(0xE0 | ((c >> 12) & 0x0F)));
                buffer.writeByte((byte)(0x80 | ((c >> 6) & 0x3F)));
                buffer.writeByte((byte)(0x80 | (c & 0x3F)));
            } else {
                int low;

                if ((++i == length) || ((low = value.charAt(i)) & 0xDC00) != 0xDC00) {
                    throw new IllegalArgumentException("String contains invalid Unicode code points");
                }

                c = 0x010000 + ((c & 0x03FF) << 10) + (low & 0x03FF);

                buffer.writeByte((byte)(0xF0 | ((c >> 18) & 0x07)));
                buffer.writeByte((byte)(0x80 | ((c >> 12) & 0x3F)));
                buffer.writeByte((byte)(0x80 | ((c >> 6) & 0x3F)));
                buffer.writeByte((byte)(0x80 | (c & 0x3F)));
            }
        }
    }
}
