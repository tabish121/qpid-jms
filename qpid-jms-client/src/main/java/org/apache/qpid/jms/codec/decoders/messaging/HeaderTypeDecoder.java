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
package org.apache.qpid.jms.codec.decoders.messaging;

import java.io.IOException;

import org.apache.qpid.jms.codec.DecoderState;
import org.apache.qpid.jms.codec.DescribedTypeDecoder;
import org.apache.qpid.jms.codec.TypeDecoder;
import org.apache.qpid.jms.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.Header;

import io.netty.buffer.ByteBuf;

/**
 * Decoder of AMQP Header types from a byte stream
 */
public class HeaderTypeDecoder implements DescribedTypeDecoder<Header>, ListTypeDecoder.ListEntryHandler<Header> {

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
    public Header readValue(ByteBuf buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof ListTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getTypeClass().getName());
        }

        ListTypeDecoder listDecoder = (ListTypeDecoder) decoder;
        Header header = new Header();

        listDecoder.readValue(buffer, state, this, header);

        return header;
    }

    @Override
    public void onListEntry(int index, Header header, ByteBuf buffer, DecoderState state) throws IOException {
        switch (index) {
            case 0:
                header.setDurable(state.getDecoder().readBoolean(buffer, state));
                break;
            case 1:
                header.setPriority(state.getDecoder().readUnsignedByte(buffer, state));
                break;
            case 2:
                header.setTtl(state.getDecoder().readUnsignedInteger(buffer, state));
                break;
            case 3:
                header.setFirstAcquirer(state.getDecoder().readBoolean(buffer, state));
                break;
            case 4:
                header.setDeliveryCount(state.getDecoder().readUnsignedInteger(buffer, state));
                break;
            default:
                throw new IllegalStateException("To many entries in Header encoding");
        }
    }
}
