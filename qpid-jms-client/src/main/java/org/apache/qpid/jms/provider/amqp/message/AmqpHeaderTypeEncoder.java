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
package org.apache.qpid.jms.provider.amqp.message;

import org.apache.qpid.jms.codec.DescribedListTypeEncoder;
import org.apache.qpid.jms.codec.EncoderState;
import org.apache.qpid.jms.codec.EncodingCodes;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP Header type values to a byte stream
 */
public class AmqpHeaderTypeEncoder implements DescribedListTypeEncoder<AmqpHeader> {

    private static final UnsignedLong descriptorCode = UnsignedLong.valueOf(0x0000000000000070L);
    private static final Symbol descriptorSymbol = Symbol.valueOf("amqp:header:list");

    @Override
    public Class<AmqpHeader> getTypeClass() {
        return AmqpHeader.class;
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
    public int getLargestEncoding() {
        return EncodingCodes.LIST8;
    }

    @Override
    public void writeElement(AmqpHeader header, int index, ByteBuf buffer, EncoderState state) {
        switch (index) {
            case 0:
                state.getEncoder().writeBoolean(buffer, state, header.getRawDurable());
                break;
            case 1:
                state.getEncoder().writeUnsignedByte(buffer, state, header.getRawPriority());
                break;
            case 2:
                state.getEncoder().writeUnsignedInteger(buffer, state, header.getRawTimeToLive());
                break;
            case 3:
                state.getEncoder().writeBoolean(buffer, state, header.getRawFirstAcquirer());
                break;
            case 4:
                state.getEncoder().writeUnsignedInteger(buffer, state, header.getRawDeliveryCount());
                break;
            default:
                throw new IllegalArgumentException("Unknown Header value index: " + index);
        }
    }

    @Override
    public int getElementCount(AmqpHeader header) {
        if (header.isDefault()) {
            return 0;
        } else {
            return 32 - Integer.numberOfLeadingZeros(header.getModifiedFlag());
        }
    }
}
