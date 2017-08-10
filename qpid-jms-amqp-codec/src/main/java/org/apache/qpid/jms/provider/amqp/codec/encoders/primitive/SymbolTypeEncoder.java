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
import org.apache.qpid.proton.amqp.Symbol;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP Symbol type values to a byte stream.
 */
public class SymbolTypeEncoder implements PrimitiveTypeEncoder<Symbol> {

    @Override
    public Class<Symbol> getTypeClass() {
        return Symbol.class;
    }

    @Override
    public void writeType(ByteBuf buffer, EncoderState state, Symbol value) {
        write(buffer, state, value, true);
    }

    @Override
    public void writeValue(ByteBuf buffer, EncoderState state, Symbol value) {
        write(buffer, state, value, false);
    }

    protected void write(ByteBuf buffer, EncoderState state, Symbol value, boolean writeEncoding) {
        if (value.length() <= 255) {
            if (writeEncoding) {
                buffer.writeByte(EncodingCodes.SYM8);
            }
            buffer.writeByte(value.length());
            value.chars().forEach(charAt -> buffer.writeByte(charAt));
        } else {
            if (writeEncoding) {
                buffer.writeByte(EncodingCodes.SYM32);
            }
            buffer.writeInt(value.length());
            value.chars().forEach(charAt -> buffer.writeByte(charAt));
        }
    }
}
