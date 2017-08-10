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
import org.apache.qpid.proton.amqp.transport.ErrorCondition;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP ErrorCondition type values to a byte stream
 */
public class ErrorConditionTypeEncoder implements DescribedListTypeEncoder<ErrorCondition> {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x000000000000001dL);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:error:list");

    @Override
    public UnsignedLong getDescriptorCode() {
        return DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<ErrorCondition> getTypeClass() {
        return ErrorCondition.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeElement(ErrorCondition error, int index, ByteBuf buffer, EncoderState state) {
        switch (index) {
            case 0:
                state.getEncoder().writeSymbol(buffer, state, error.getCondition());
                break;
            case 1:
                state.getEncoder().writeString(buffer, state, error.getDescription());
                break;
            case 2:
                state.getEncoder().writeMap(buffer, state, error.getInfo());
                break;
            default:
                throw new IllegalArgumentException("Unknown ErrorCondition value index: " + index);
        }
    }

    @Override
    public int getElementCount(ErrorCondition error) {
        if (error.getInfo() != null) {
            return 3;
        } else if (error.getDescription() != null) {
            return 2;
        } else {
            return 1;
        }
    }
}
