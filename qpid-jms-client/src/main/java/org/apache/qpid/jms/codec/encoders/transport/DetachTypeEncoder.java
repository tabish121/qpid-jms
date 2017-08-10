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
package org.apache.qpid.jms.codec.encoders.transport;

import org.apache.qpid.jms.codec.DescribedListTypeEncoder;
import org.apache.qpid.jms.codec.EncoderState;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.transport.Detach;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP Detach type values to a byte stream.
 */
public class DetachTypeEncoder implements DescribedListTypeEncoder<Detach> {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000016L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:detach:list");

    @Override
    public UnsignedLong getDescriptorCode() {
        return DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Detach> getTypeClass() {
        return Detach.class;
    }

    @Override
    public void writeElement(Detach detach, int index, ByteBuf buffer, EncoderState state) {
        switch (index) {
            case 0:
                state.getEncoder().writeUnsignedInteger(buffer, state, detach.getHandle());
                break;
            case 1:
                state.getEncoder().writeBoolean(buffer, state, detach.getClosed());
                break;
            case 2:
                state.getEncoder().writeObject(buffer, state, detach.getError());
                break;
            default:
                throw new IllegalArgumentException("Unknown Detach value index: " + index);
        }
    }

    @Override
    public int getElementCount(Detach detach) {
        if (detach.getError() != null) {
            return 3;
        } else if (detach.getClosed()) {
            return 2;
        } else {
            return 1;
        }
    }
}
