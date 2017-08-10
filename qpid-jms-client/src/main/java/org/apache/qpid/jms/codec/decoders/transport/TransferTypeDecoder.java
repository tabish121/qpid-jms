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
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.Transfer;

import io.netty.buffer.ByteBuf;

/**
 * Decoder of AMQP Transfer type values from a byte stream
 */
public class TransferTypeDecoder implements DescribedTypeDecoder<Transfer>, ListEntryHandler<Transfer> {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000014L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:transfer:list");

    @Override
    public Class<Transfer> getTypeClass() {
        return Transfer.class;
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
    public Transfer readValue(ByteBuf buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof ListTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getTypeClass().getName());
        }

        ListTypeDecoder listDecoder = (ListTypeDecoder) decoder;
        Transfer transfer = new Transfer();

        listDecoder.readValue(buffer, state, this, transfer);

        return transfer;
    }

    @Override
    public void onListEntry(int index, Transfer transfer, ByteBuf buffer, DecoderState state) throws IOException {
        switch (index) {
            case 0:
                transfer.setHandle(state.getDecoder().readUnsignedInteger(buffer, state));
                break;
            case 1:
                transfer.setDeliveryId(state.getDecoder().readUnsignedInteger(buffer, state));
                break;
            case 2:
                transfer.setDeliveryTag(state.getDecoder().readBinary(buffer, state));
                break;
            case 3:
                transfer.setMessageFormat(state.getDecoder().readUnsignedInteger(buffer, state));
                break;
            case 4:
                transfer.setSettled(state.getDecoder().readBoolean(buffer, state));
                break;
            case 5:
                transfer.setMore(Boolean.TRUE.equals(state.getDecoder().readBoolean(buffer, state)));
                break;
            case 6:
                UnsignedByte rcvSettleMode = state.getDecoder().readUnsignedByte(buffer, state);
                transfer.setRcvSettleMode(rcvSettleMode == null ? null : ReceiverSettleMode.values()[rcvSettleMode.intValue()]);
                break;
            case 7:
                transfer.setState(state.getDecoder().readObject(buffer, state, DeliveryState.class));
                break;
            case 8:
                transfer.setResume(Boolean.TRUE.equals(state.getDecoder().readBoolean(buffer, state)));
                break;
            case 9:
                transfer.setMore(Boolean.TRUE.equals(state.getDecoder().readBoolean(buffer, state)));
                break;
            case 10:
                transfer.setBatchable(Boolean.TRUE.equals(state.getDecoder().readBoolean(buffer, state)));
                break;
            default:
                throw new IllegalStateException("To many entries in Transfer encoding");
        }
    }
}
