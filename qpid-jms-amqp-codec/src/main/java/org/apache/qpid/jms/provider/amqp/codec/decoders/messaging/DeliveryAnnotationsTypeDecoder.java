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
package org.apache.qpid.jms.provider.amqp.codec.decoders.messaging;

import java.io.IOException;
import java.util.Map;

import org.apache.qpid.jms.provider.amqp.codec.DecoderState;
import org.apache.qpid.jms.provider.amqp.codec.DescribedTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.MapTypeDecoder;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;

import io.netty.buffer.ByteBuf;

/**
 * Decoder of AMQP Delivery Annotations type values from a byte stream.
 */
public class DeliveryAnnotationsTypeDecoder implements DescribedTypeDecoder<DeliveryAnnotations> {

    private static final UnsignedLong descriptorCode = UnsignedLong.valueOf(0x0000000000000071L);
    private static final Symbol descriptorSymbol = Symbol.valueOf("amqp:delivery-annotations:map");

    @Override
    public Class<DeliveryAnnotations> getTypeClass() {
        return DeliveryAnnotations.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return descriptorCode;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return descriptorSymbol;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public DeliveryAnnotations readValue(ByteBuf buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof MapTypeDecoder)) {
            throw new IOException("Expected Map type indicator but got decoder for type: " + decoder.getClass().getSimpleName());
        }

        MapTypeDecoder mapDecoder = (MapTypeDecoder) decoder;

        Map map = mapDecoder.readValue(buffer, state);
        return new DeliveryAnnotations(map);
    }
}
