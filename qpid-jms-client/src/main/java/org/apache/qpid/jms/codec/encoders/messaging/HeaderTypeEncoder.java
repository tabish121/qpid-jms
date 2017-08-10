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
package org.apache.qpid.jms.codec.encoders.messaging;

import org.apache.qpid.jms.codec.DescribedListTypeEncoder;
import org.apache.qpid.jms.codec.EncoderState;
import org.apache.qpid.jms.codec.EncodingCodes;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.Header;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP Header type values to a byte stream
 */
public class HeaderTypeEncoder implements DescribedListTypeEncoder<Header> {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000070L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:header:list");

    @Override
    public Class<Header> getTypeClass() {
        return Header.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return DESCRIPTOR_SYMBOL;
    }

    @Override
    public int getLargestEncoding() {
        return EncodingCodes.LIST8;
    }

    @Override
    public void writeElement(Header header, int index, ByteBuf buffer, EncoderState state) {
        switch (index) {
            case 0:
                state.getEncoder().writeBoolean(buffer, state, header.getDurable());
                break;
            case 1:
                state.getEncoder().writeUnsignedByte(buffer, state, header.getPriority());
                break;
            case 2:
                state.getEncoder().writeUnsignedInteger(buffer, state, header.getTtl());
                break;
            case 3:
                state.getEncoder().writeBoolean(buffer, state, header.getFirstAcquirer());
                break;
            case 4:
                state.getEncoder().writeUnsignedInteger(buffer, state, header.getDeliveryCount());
                break;
            default:
                throw new IllegalArgumentException("Unknown Header value index: " + index);
        }
    }

    @Override
    public int getElementCount(Header header) {
        if (header.getDeliveryCount() != null) {
            return 5;
        } else if (header.getFirstAcquirer() != null) {
            return 4;
        } else if (header.getTtl() != null) {
            return 3;
        } else if (header.getPriority() != null) {
            return 2;
        } else if (header.getDurable() != null) {
            return 1;
        } else {
            return 0;
        }
    }
}
