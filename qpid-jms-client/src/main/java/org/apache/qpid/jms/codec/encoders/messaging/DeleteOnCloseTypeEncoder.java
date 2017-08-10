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
import org.apache.qpid.jms.codec.EncodingCodes;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.DeleteOnClose;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP DeleteOnClose type values to a byte stream.
 */
public class DeleteOnCloseTypeEncoder implements DescribedListTypeEncoder<DeleteOnClose> {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x000000000000002bL);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:delete-on-close:list");

    @Override
    public UnsignedLong getDescriptorCode() {
        return DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<DeleteOnClose> getTypeClass() {
        return DeleteOnClose.class;
    }

    @Override
    public int getLargestEncoding() {
        return EncodingCodes.LIST0 & 0xff;
    }

    @Override
    public void writeElement(DeleteOnClose source, int index, ByteBuf buffer, EncoderState state) {
    }

    @Override
    public int getElementCount(DeleteOnClose value) {
        return 0;
    }
}
