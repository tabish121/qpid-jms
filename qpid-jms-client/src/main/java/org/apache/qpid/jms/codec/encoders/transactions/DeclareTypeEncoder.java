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
package org.apache.qpid.jms.codec.encoders.transactions;

import org.apache.qpid.jms.codec.DescribedListTypeEncoder;
import org.apache.qpid.jms.codec.EncoderState;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.transaction.Declare;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP Declare type values to a byte stream.
 */
public class DeclareTypeEncoder implements DescribedListTypeEncoder<Declare> {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000031L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:declare:list");

    @Override
    public UnsignedLong getDescriptorCode() {
        return DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Declare> getTypeClass() {
        return Declare.class;
    }

    @Override
    public void writeElement(Declare declare, int index, ByteBuf buffer, EncoderState state) {
        switch (index) {
            case 0:
                state.getEncoder().writeObject(buffer, state, declare.getGlobalId());
                break;
            default:
                throw new IllegalArgumentException("Unknown Declare value index: " + index);
        }
    }

    @Override
    public int getElementCount(Declare declare) {
        if (declare.getGlobalId() != null) {
            return 1;
        } else {
            return 0;
        }
    }
}
