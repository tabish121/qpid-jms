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
import java.util.Map;

import org.apache.qpid.jms.codec.DecoderState;
import org.apache.qpid.jms.codec.DescribedTypeDecoder;
import org.apache.qpid.jms.codec.TypeDecoder;
import org.apache.qpid.jms.codec.decoders.primitives.MapTypeDecoder;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;

import io.netty.buffer.ByteBuf;

/**
 * Decoder of AMQP Message Annotations type values from a byte stream.
 */
public class MessageAnnotationsTypeDecoder implements DescribedTypeDecoder<MessageAnnotations> {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000072L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:message-annotations:map");

    @Override
    public Class<MessageAnnotations> getTypeClass() {
        return MessageAnnotations.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return DESCRIPTOR_SYMBOL;
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public MessageAnnotations readValue(ByteBuf buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof MapTypeDecoder)) {
            throw new IOException("Expected Map type indicator but got decoder for type: " + decoder.getClass().getSimpleName());
        }

        MapTypeDecoder mapDecoder = (MapTypeDecoder) decoder;

        Map<Symbol, Object> map = mapDecoder.readValue(buffer, state);
        return new MessageAnnotations(map);
    }
}
