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
package org.apache.qpid.jms.codec.decoders.transport;

import java.io.IOException;

import org.apache.qpid.jms.codec.DecoderState;
import org.apache.qpid.jms.codec.DescribedTypeDecoder;
import org.apache.qpid.jms.codec.TypeDecoder;
import org.apache.qpid.jms.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.jms.codec.decoders.primitives.ListTypeDecoder.ListEntryHandler;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.transport.Flow;

import io.netty.buffer.ByteBuf;

/**
 * Decoder of AMQP Flow type values from a byte stream.
 */
public class FlowTypeDecoder implements DescribedTypeDecoder<Flow>, ListEntryHandler<Flow> {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000013L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:flow:list");

    @Override
    public Class<Flow> getTypeClass() {
        return Flow.class;
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
    public Flow readValue(ByteBuf buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof ListTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getTypeClass().getName());
        }

        ListTypeDecoder listDecoder = (ListTypeDecoder) decoder;
        Flow flow = new Flow();

        listDecoder.readValue(buffer, state, this, flow);

        return flow;
    }

    @Override
    public void onListEntry(int index, Flow flow, ByteBuf buffer, DecoderState state) throws IOException {
        switch (index) {
            case 0:
                flow.setNextIncomingId(state.getDecoder().readUnsignedInteger(buffer, state));
                break;
            case 1:
                flow.setIncomingWindow(state.getDecoder().readUnsignedInteger(buffer, state));
                break;
            case 2:
                flow.setNextOutgoingId(state.getDecoder().readUnsignedInteger(buffer, state));
                break;
            case 3:
                flow.setOutgoingWindow(state.getDecoder().readUnsignedInteger(buffer, state));
                break;
            case 4:
                flow.setHandle(state.getDecoder().readUnsignedInteger(buffer, state));
                break;
            case 5:
                flow.setDeliveryCount(state.getDecoder().readUnsignedInteger(buffer, state));
                break;
            case 6:
                flow.setLinkCredit(state.getDecoder().readUnsignedInteger(buffer, state));
                break;
            case 7:
                flow.setAvailable(state.getDecoder().readUnsignedInteger(buffer, state));
                break;
            case 8:
                flow.setDrain(Boolean.TRUE.equals(state.getDecoder().readBoolean(buffer, state)));
                break;
            case 9:
                flow.setEcho(Boolean.TRUE.equals(state.getDecoder().readBoolean(buffer, state)));
                break;
            case 10:
                flow.setProperties(state.getDecoder().readMap(buffer, state));
                break;
            default:
                throw new IllegalStateException("To many entries in Flow encoding");
        }
    }
}
