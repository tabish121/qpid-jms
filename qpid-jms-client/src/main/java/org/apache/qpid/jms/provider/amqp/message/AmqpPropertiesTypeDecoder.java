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

import java.io.IOException;

import org.apache.qpid.jms.provider.amqp.codec.DecoderState;
import org.apache.qpid.jms.provider.amqp.codec.DescribedTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.ListTypeDecoder;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;

import io.netty.buffer.ByteBuf;

/**
 * Decoder of AMQP Property types from a byte stream
 */
public class AmqpPropertiesTypeDecoder implements DescribedTypeDecoder<AmqpProperties>, ListTypeDecoder.ListEntryHandler {

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
    public AmqpProperties readValue(ByteBuf buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof ListTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getTypeClass().getName());
        }

        ListTypeDecoder listDecoder = (ListTypeDecoder) decoder;
        AmqpProperties properties = new AmqpProperties();

        listDecoder.readValue(buffer, state, this, properties);

        return properties;
    }

    @Override
    public void onListEntry(int index, Object target, ByteBuf buffer, DecoderState state) throws IOException {
        AmqpProperties properties = (AmqpProperties) target;

        switch (index) {
            case 0:
                properties.setMessageId(state.getDecoder().readObject(buffer, state));
                break;
            case 1:
                properties.setUserId(state.getDecoder().readBinary(buffer, state));
                break;
            case 2:
                properties.setTo(state.getDecoder().readString(buffer, state));
                break;
            case 3:
                properties.setSubject(state.getDecoder().readString(buffer, state));
                break;
            case 4:
                properties.setReplyTo(state.getDecoder().readString(buffer, state));
                break;
            case 5:
                properties.setCorrelationId(state.getDecoder().readObject(buffer, state));
                break;
            case 6:
                properties.setContentType(state.getDecoder().readSymbol(buffer, state));
                break;
            case 7:
                properties.setContentEncoding(state.getDecoder().readSymbol(buffer, state));
                break;
            case 8:
                properties.setAbsoluteExpiryTime(state.getDecoder().readTimestamp(buffer, state));
                break;
            case 9:
                properties.setCreationTime(state.getDecoder().readTimestamp(buffer, state));
                break;
            case 10:
                properties.setGroupId(state.getDecoder().readString(buffer, state));
                break;
            case 11:
                properties.setGroupSequence(state.getDecoder().readUnsignedInteger(buffer, state));
                break;
            case 12:
                properties.setReplyToGroupId(state.getDecoder().readString(buffer, state));
                break;
            default:
                throw new IllegalStateException("To many entries in Properties encoding");
        }
    }
}
