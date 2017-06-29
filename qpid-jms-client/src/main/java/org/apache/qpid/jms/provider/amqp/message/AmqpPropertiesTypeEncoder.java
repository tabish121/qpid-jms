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

import org.apache.qpid.jms.provider.amqp.codec.DescribedListTypeEncoder;
import org.apache.qpid.jms.provider.amqp.codec.EncoderState;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP Properties type values to a byte stream
 */
public class AmqpPropertiesTypeEncoder implements DescribedListTypeEncoder<AmqpProperties> {

    private static final UnsignedLong descriptorCode = UnsignedLong.valueOf(0x0000000000000073L);
    private static final Symbol descriptorSymbol = Symbol.valueOf("amqp:properties:list");

    @Override
    public UnsignedLong getDescriptorCode() {
        return descriptorCode;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return descriptorSymbol;
    }

    @Override
    public Class<AmqpProperties> getTypeClass() {
        return AmqpProperties.class;
    }

    @Override
    public void writeElement(AmqpProperties properties, int index, ByteBuf buffer, EncoderState state) {
        switch (index) {
            case 0:
                state.getEncoder().writeObject(buffer, state, properties.getMessageId());
                break;
            case 1:
                state.getEncoder().writeBinary(buffer, state, properties.getUserId());
                break;
            case 2:
                state.getEncoder().writeString(buffer, state, properties.getTo());
                break;
            case 3:
                state.getEncoder().writeString(buffer, state, properties.getSubject());
                break;
            case 4:
                state.getEncoder().writeString(buffer, state, properties.getReplyTo());
                break;
            case 5:
                state.getEncoder().writeObject(buffer, state, properties.getCorrelationId());
                break;
            case 6:
                state.getEncoder().writeSymbol(buffer, state, properties.getRawContentType());
                break;
            case 7:
                state.getEncoder().writeSymbol(buffer, state, properties.getRawContentEncoding());
                break;
            case 8:
                state.getEncoder().writeTimestamp(buffer, state, properties.getAbsoluteExpiryTime());
                break;
            case 9:
                state.getEncoder().writeTimestamp(buffer, state, properties.getCreationTime());
                break;
            case 10:
                state.getEncoder().writeString(buffer, state, properties.getGroupId());
                break;
            case 11:
                state.getEncoder().writeUnsignedInteger(buffer, state, properties.getRawGroupSequence());
                break;
            case 12:
                state.getEncoder().writeString(buffer, state, properties.getReplyToGroupId());
                break;
            default:
                throw new IllegalArgumentException("Unknown Properties value index: " + index);
        }
    }

    @Override
    public int getElementCount(AmqpProperties properties) {
        if (properties.isDefault()) {
            return 0;
        } else {
            return 32 - Integer.numberOfLeadingZeros(properties.getModifiedFlag());
        }
    }
}
