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
import org.apache.qpid.jms.codec.EncodingCodes;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.DeleteOnClose;

import io.netty.buffer.ByteBuf;

/**
 * Decoder of AMQP DeleteOnClose type values from a byte stream
 */
public class DeleteOnCloseTypeDecoder implements DescribedTypeDecoder<DeleteOnClose> {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x000000000000002bL);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:delete-on-close:list");

    @Override
    public Class<DeleteOnClose> getTypeClass() {
        return DeleteOnClose.class;
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
    public DeleteOnClose readValue(ByteBuf buffer, DecoderState state) throws IOException {
        byte code = buffer.readByte();

        if (code != EncodingCodes.LIST0) {
            throw new IOException("Expected List0 type indicator but got code for type: " + code);
        }

        return DeleteOnClose.getInstance();
    }
}
