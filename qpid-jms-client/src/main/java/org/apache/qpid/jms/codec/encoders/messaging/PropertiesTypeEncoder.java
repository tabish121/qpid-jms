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
import org.apache.qpid.proton.amqp.messaging.Properties;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP Properties type value to a byte stream.
 */
public class PropertiesTypeEncoder implements DescribedListTypeEncoder<Properties> {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000073L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:properties:list");

    @Override
    public UnsignedLong getDescriptorCode() {
        return DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Properties> getTypeClass() {
        return Properties.class;
    }

    @Override
    public void writeElement(Properties properties, int index, ByteBuf buffer, EncoderState state) {
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
                state.getEncoder().writeSymbol(buffer, state, properties.getContentType());
                break;
            case 7:
                state.getEncoder().writeSymbol(buffer, state, properties.getContentEncoding());
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
                state.getEncoder().writeUnsignedInteger(buffer, state, properties.getGroupSequence());
                break;
            case 12:
                state.getEncoder().writeString(buffer, state, properties.getReplyToGroupId());
                break;
            default:
                throw new IllegalArgumentException("Unknown Properties value index: " + index);
        }
    }

    @Override
    public int getElementCount(Properties properties) {
        if (properties.getReplyToGroupId() != null) {
            return 13;
        } else if (properties.getGroupSequence() != null) {
            return 12;
        } else if (properties.getGroupId() != null) {
            return 11;
        } else if (properties.getCreationTime() != null) {
            return 10;
        } else if (properties.getAbsoluteExpiryTime() != null) {
            return 9;
        } else if (properties.getContentEncoding() != null) {
            return 8;
        } else if (properties.getContentType() != null) {
            return 7;
        } else if (properties.getCorrelationId() != null) {
            return 6;
        } else if (properties.getReplyTo() != null) {
            return 5;
        } else if (properties.getSubject() != null) {
            return 4;
        } else if (properties.getTo() != null) {
            return 3;
        } else if (properties.getUserId() != null) {
            return 2;
        } else if (properties.getMessageId() != null) {
            return 1;
        }

        return 0;
    }
}
