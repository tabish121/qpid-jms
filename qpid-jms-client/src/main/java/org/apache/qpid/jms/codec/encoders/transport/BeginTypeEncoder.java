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
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.transport.Begin;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP Begin type values to a byte stream.
 */
public class BeginTypeEncoder implements DescribedListTypeEncoder<Begin> {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000011L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:begin:list");

    @Override
    public UnsignedLong getDescriptorCode() {
        return DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Begin> getTypeClass() {
        return Begin.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeElement(Begin begin, int index, ByteBuf buffer, EncoderState state) {
        switch (index) {
            case 0:
                state.getEncoder().writeUnsignedShort(buffer, state, begin.getRemoteChannel());
                break;
            case 1:
                state.getEncoder().writeUnsignedInteger(buffer, state, begin.getNextOutgoingId());
                break;
            case 2:
                state.getEncoder().writeUnsignedInteger(buffer, state, begin.getIncomingWindow());
                break;
            case 3:
                state.getEncoder().writeUnsignedInteger(buffer, state, begin.getOutgoingWindow());
                break;
            case 4:
                state.getEncoder().writeUnsignedInteger(buffer, state, begin.getHandleMax());
                break;
            case 5:
                state.getEncoder().writeArray(buffer, state, begin.getOfferedCapabilities());
                break;
            case 6:
                state.getEncoder().writeArray(buffer, state, begin.getDesiredCapabilities());
                break;
            case 7:
                state.getEncoder().writeMap(buffer, state, begin.getProperties());
                break;
            default:
                throw new IllegalArgumentException("Unknown Begin value index: " + index);
        }
    }

    @Override
    public int getElementCount(Begin begin) {
        if (begin.getProperties() != null) {
            return 8;
        } else if (begin.getDesiredCapabilities() != null) {
            return 7;
        } else if (begin.getOfferedCapabilities() != null) {
            return 6;
        } else if (begin.getHandleMax() != null && !begin.getHandleMax().equals(UnsignedInteger.MAX_VALUE)) {
            return 5;
        } else {
            return 4;
        }
    }
}
