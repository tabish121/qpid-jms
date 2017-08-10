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
import org.apache.qpid.proton.amqp.messaging.DeleteOnNoLinks;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP DeleteOnNoLinks type values to a byte stream
 */
public class DeleteOnNoLinksTypeEncoder implements DescribedListTypeEncoder<DeleteOnNoLinks> {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x000000000000002cL);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:delete-on-no-links:list");

    @Override
    public UnsignedLong getDescriptorCode() {
        return DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<DeleteOnNoLinks> getTypeClass() {
        return DeleteOnNoLinks.class;
    }

    @Override
    public int getLargestEncoding() {
        return EncodingCodes.LIST0 & 0xff;
    }

    @Override
    public void writeElement(DeleteOnNoLinks source, int index, ByteBuf buffer, EncoderState state) {
    }

    @Override
    public int getElementCount(DeleteOnNoLinks value) {
        return 0;
    }
}
