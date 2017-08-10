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
import org.apache.qpid.jms.codec.decoders.primitives.ListTypeDecoder.ListEntryHandler;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.Outcome;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;

import io.netty.buffer.ByteBuf;

/**
 * Decoder of AMQP Source type values from a byte stream.
 */
public class SourceTypeDecoder implements DescribedTypeDecoder<Source>, ListEntryHandler<Source> {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000028L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:source:list");

    @Override
    public Class<Source> getTypeClass() {
        return Source.class;
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
    public Source readValue(ByteBuf buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof ListTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getTypeClass().getName());
        }

        ListTypeDecoder listDecoder = (ListTypeDecoder) decoder;
        Source source = new Source();

        listDecoder.readValue(buffer, state, this, source);

        return source;
    }

    @Override
    public void onListEntry(int index, Source source, ByteBuf buffer, DecoderState state) throws IOException {
        switch (index) {
            case 0:
                source.setAddress(state.getDecoder().readString(buffer, state));
                break;
            case 1:
                UnsignedInteger durability = state.getDecoder().readUnsignedInteger(buffer, state);
                source.setDurable(durability == null ? TerminusDurability.NONE : TerminusDurability.get(durability));
                break;
            case 2:
                Symbol expiryPolicy = state.getDecoder().readSymbol(buffer, state);
                source.setExpiryPolicy(expiryPolicy == null ? TerminusExpiryPolicy.SESSION_END : TerminusExpiryPolicy.valueOf(expiryPolicy));
                break;
            case 3:
                UnsignedInteger timeout = state.getDecoder().readUnsignedInteger(buffer, state);
                source.setTimeout(timeout == null ? UnsignedInteger.ZERO : timeout);
                break;
            case 4:
                source.setDynamic(Boolean.TRUE.equals(state.getDecoder().readBoolean(buffer, state)));
                break;
            case 5:
                source.setDynamicNodeProperties(state.getDecoder().readMap(buffer, state));
                break;
            case 6:
                source.setDistributionMode(state.getDecoder().readSymbol(buffer, state));
                break;
            case 7:
                source.setFilter(state.getDecoder().readMap(buffer, state));
                break;
            case 8:
                source.setDefaultOutcome(state.getDecoder().readObject(buffer, state, Outcome.class));
                break;
            case 9:
                source.setOutcomes(state.getDecoder().readMultiple(buffer, state, Symbol.class));
                break;
            case 10:
                source.setCapabilities(state.getDecoder().readMultiple(buffer, state, Symbol.class));
                break;
            default:
                throw new IllegalStateException("To many entries in Source encoding");
        }
    }
}
