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

import org.apache.qpid.jms.codec.EncoderState;
import org.apache.qpid.jms.codec.EncodingCodes;
import org.apache.qpid.jms.codec.PrimitiveTypeEncoder;
import org.apache.qpid.proton.amqp.Decimal64;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP Decimal64 type values to a byte stream
 */
public class Decimal64TypeEncoder implements PrimitiveTypeEncoder<Decimal64> {

    @Override
    public Class<Decimal64> getTypeClass() {
        return Decimal64.class;
    }

    @Override
    public void writeType(ByteBuf buffer, EncoderState state, Decimal64 value) {
        buffer.writeByte(EncodingCodes.DECIMAL64);
        buffer.writeLong(value.getBits());
    }

    @Override
    public void writeValue(ByteBuf buffer, EncoderState state, Decimal64 value) {
        buffer.writeLong(value.getBits());
    }
}
