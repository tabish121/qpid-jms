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
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.Source;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP Source type values to a byte stream.
 */
public class SourceTypeEncoder implements DescribedListTypeEncoder<Source> {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000028L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:source:list");

    @Override
    public UnsignedLong getDescriptorCode() {
        return DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Source> getTypeClass() {
        return Source.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeElement(Source source, int index, ByteBuf buffer, EncoderState state) {
        switch (index) {
            case 0:
                state.getEncoder().writeString(buffer, state, source.getAddress());
                break;
            case 1:
                state.getEncoder().writeObject(buffer, state, source.getDurable());
                break;
            case 2:
                state.getEncoder().writeObject(buffer, state, source.getExpiryPolicy());
                break;
            case 3:
                state.getEncoder().writeUnsignedInteger(buffer, state, source.getTimeout());
                break;
            case 4:
                state.getEncoder().writeBoolean(buffer, state, source.getDynamic());
                break;
            case 5:
                state.getEncoder().writeMap(buffer, state, source.getDynamicNodeProperties());
                break;
            case 6:
                state.getEncoder().writeSymbol(buffer, state, source.getDistributionMode());
                break;
            case 7:
                state.getEncoder().writeMap(buffer, state, source.getFilter());
                break;
            case 8:
                state.getEncoder().writeObject(buffer, state, source.getDefaultOutcome());
                break;
            case 9:
                state.getEncoder().writeArray(buffer, state, source.getOutcomes());
                break;
            case 10:
                state.getEncoder().writeArray(buffer, state, source.getCapabilities());
                break;
            default:
                throw new IllegalArgumentException("Unknown Source value index: " + index);
        }
    }

    @Override
    public int getElementCount(Source source) {
        if (source.getCapabilities() != null) {
            return 11;
        } else if (source.getOutcomes() != null) {
            return 10;
        } else if (source.getDefaultOutcome() != null) {
            return 9;
        } else if (source.getFilter() != null) {
            return 8;
        } else if (source.getDistributionMode() != null) {
            return 7;
        } else if (source.getDynamicNodeProperties() != null) {
            return 6;
        } else if (source.getDynamic()) {
            return 5;
        } else if (source.getTimeout() != null && !source.getTimeout().equals(UnsignedInteger.ZERO)) {
            return 4;
        } else if (source.getExpiryPolicy() != null) {
            return 3;
        } else if (source.getDurable() != null) {
            return 2;
        } else if (source.getAddress() != null) {
            return 1;
        } else {
            return 0;
        }
    }
}
