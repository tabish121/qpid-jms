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
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.Modified;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP Modified type values to a byte stream.
 */
public class ModifiedTypeEncoder implements DescribedListTypeEncoder<Modified> {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000027L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:modified:list");

    @Override
    public UnsignedLong getDescriptorCode() {
        return DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Modified> getTypeClass() {
        return Modified.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeElement(Modified source, int index, ByteBuf buffer, EncoderState state) {
        switch (index) {
            case 0:
                state.getEncoder().writeBoolean(buffer, state, source.getDeliveryFailed());
                break;
            case 1:
                state.getEncoder().writeBoolean(buffer, state, source.getUndeliverableHere());
                break;
            case 2:
                state.getEncoder().writeMap(buffer, state, source.getMessageAnnotations());
                break;
            default:
                throw new IllegalArgumentException("Unknown Modified value index: " + index);
        }
    }

    @Override
    public int getElementCount(Modified value) {
        if (value.getMessageAnnotations() != null) {
            return 3;
        } else if (value.getUndeliverableHere() != null) {
            return 2;
        } else if (value.getDeliveryFailed() != null) {
            return 1;
        } else {
            return 0;
        }
    }
}
