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
package org.apache.qpid.jms.codec;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Decimal128;
import org.apache.qpid.proton.amqp.Decimal32;
import org.apache.qpid.proton.amqp.Decimal64;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.UnsignedShort;

import io.netty.buffer.ByteBuf;

/**
 * Decode AMQP types from a byte stream
 */
public interface Decoder {

    // TODO - Decide if we should provide read methods that accept a default
    //        value to return when the read in value is a null encoding.

    DecoderState newDecoderState();

    Boolean readBoolean(ByteBuf buffer, DecoderState state) throws IOException;

    Byte readByte(ByteBuf buffer, DecoderState state) throws IOException;

    UnsignedByte readUnsignedByte(ByteBuf buffer, DecoderState state) throws IOException;

    Character readCharacter(ByteBuf buffer, DecoderState state) throws IOException;

    Decimal32 readDecimal32(ByteBuf buffer, DecoderState state) throws IOException;

    Decimal64 readDecimal64(ByteBuf buffer, DecoderState state) throws IOException;

    Decimal128 readDecimal128(ByteBuf buffer, DecoderState state) throws IOException;

    Short readShort(ByteBuf buffer, DecoderState state) throws IOException;

    UnsignedShort readUnsignedShort(ByteBuf buffer, DecoderState state) throws IOException;

    Integer readInteger(ByteBuf buffer, DecoderState state) throws IOException;

    UnsignedInteger readUnsignedInteger(ByteBuf buffer, DecoderState state) throws IOException;

    Long readLong(ByteBuf buffer, DecoderState state) throws IOException;

    UnsignedLong readUnsignedLong(ByteBuf buffer, DecoderState state) throws IOException;

    Float readFloat(ByteBuf buffer, DecoderState state) throws IOException;

    Double readDouble(ByteBuf buffer, DecoderState state) throws IOException;

    Binary readBinary(ByteBuf buffer, DecoderState state) throws IOException;

    String readString(ByteBuf buffer, DecoderState state) throws IOException;

    Symbol readSymbol(ByteBuf buffer, DecoderState state) throws IOException;

    Long readTimestamp(ByteBuf buffer, DecoderState state) throws IOException;

    UUID readUUID(ByteBuf buffer, DecoderState state) throws IOException;

    Object readObject(ByteBuf buffer, DecoderState state) throws IOException;

    <T> T readObject(ByteBuf buffer, DecoderState state, final Class<T> clazz) throws IOException;

    <T> T[] readMultiple(ByteBuf buffer, DecoderState state, final Class<T> clazz) throws IOException;

    <K,V> Map<K, V> readMap(ByteBuf buffer, DecoderState state) throws IOException;

    <V> List<V> readList(ByteBuf buffer, DecoderState state) throws IOException;

    TypeDecoder<?> readNextTypeDecoder(ByteBuf buffer, DecoderState state) throws IOException;

    TypeDecoder<?> peekNextTypeDecoder(ByteBuf buffer, DecoderState state) throws IOException;

    <V> Decoder registerDescribedTypeDecoder(DescribedTypeDecoder<V> decoder);
    <V> Decoder registerPrimitiveTypeDecoder(PrimitiveTypeDecoder<V> decoder);

    TypeDecoder<?> getTypeDecoder(Object instance);

}
