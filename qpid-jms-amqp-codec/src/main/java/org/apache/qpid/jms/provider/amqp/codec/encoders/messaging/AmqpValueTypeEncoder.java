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
package org.apache.qpid.jms.provider.amqp.codec.encoders.messaging;

import org.apache.qpid.jms.provider.amqp.codec.DescribedTypeEncoder;
import org.apache.qpid.jms.provider.amqp.codec.EncoderState;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP Value type values to a byte stream.
 */
public class AmqpValueTypeEncoder implements DescribedTypeEncoder<AmqpValue> {

    private static final UnsignedLong descriptorCode = UnsignedLong.valueOf(0x0000000000000077L);
    private static final Symbol descriptorSymbol = Symbol.valueOf("amqp:amqp-value:*");

    @Override
    public Class<AmqpValue> getTypeClass() {
        return AmqpValue.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return descriptorCode;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return descriptorSymbol;
    }

    @Override
    public void writeValue(ByteBuf buffer, EncoderState state, AmqpValue value) {
        state.getEncoder().writeObject(buffer, state, value.getValue());
    }
}
