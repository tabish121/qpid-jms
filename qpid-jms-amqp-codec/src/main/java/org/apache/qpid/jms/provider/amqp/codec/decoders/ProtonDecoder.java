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
package org.apache.qpid.jms.provider.amqp.codec.decoders;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.jms.provider.amqp.codec.Decoder;
import org.apache.qpid.jms.provider.amqp.codec.DecoderState;
import org.apache.qpid.jms.provider.amqp.codec.DescribedTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.EncodingCodes;
import org.apache.qpid.jms.provider.amqp.codec.PrimitiveArrayTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.PrimitiveTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Binary32TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Binary8TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.BooleanFalseTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.BooleanTrueTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.BooleanTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.ByteTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.CharacterTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Decimal128TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Decimal32TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Decimal64TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.DoubleTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.FloatTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Integer32TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Integer8TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Long8TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.LongTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.ShortTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.String32TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.String8TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Symbol32TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Symbol8TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.TimestampTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.UUIDTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.UnsignedByteTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.UnsignedInteger0TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.UnsignedInteger32TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.UnsignedInteger8TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.UnsignedLong0TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.UnsignedLong64TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.UnsignedLong8TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.UnsignedShortTypeDecoder;
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
 * The default AMQP Decoder implementation.
 */
public class ProtonDecoder implements Decoder {

    // The decoders for primitives are fixed and cannot be altered by users who want
    // to register custom decoders.
    private PrimitiveTypeDecoder<?>[] primitiveDecoders = new PrimitiveTypeDecoder[256];

    // Registry of decoders for described types which can be updated with user defined
    // decoders as well as the default decoders.
    private Map<Object, DescribedTypeDecoder<?>> describedTypeDecoders = new HashMap<>();

    private final Binary8TypeDecoder binary8Decoder = new Binary8TypeDecoder();
    private final Binary32TypeDecoder binary32Decoder = new Binary32TypeDecoder();
    private final BooleanTrueTypeDecoder trueDecoder = new BooleanTrueTypeDecoder();
    private final BooleanFalseTypeDecoder falseDecoder = new BooleanFalseTypeDecoder();
    private final BooleanTypeDecoder booleanDecoder = new BooleanTypeDecoder();
    private final ByteTypeDecoder byteDecoder = new ByteTypeDecoder();
    private final CharacterTypeDecoder charDecoder = new CharacterTypeDecoder();
    private final Decimal128TypeDecoder decimal128Decoder = new Decimal128TypeDecoder();
    private final Decimal64TypeDecoder decimal64Decoder = new Decimal64TypeDecoder();
    private final Decimal32TypeDecoder decimal32Decoder = new Decimal32TypeDecoder();
    private final DoubleTypeDecoder doubleDecoder = new DoubleTypeDecoder();
    private final FloatTypeDecoder floatDecoder = new FloatTypeDecoder();
    private final Integer8TypeDecoder int8Decoder = new Integer8TypeDecoder();
    private final Integer32TypeDecoder int32Decoder = new Integer32TypeDecoder();
    private final Long8TypeDecoder long8Decoder = new Long8TypeDecoder();
    private final LongTypeDecoder longDecoder = new LongTypeDecoder();
    private final ShortTypeDecoder shortDecoder = new ShortTypeDecoder();
    private final String8TypeDecoder string8Decoder = new String8TypeDecoder();
    private final String32TypeDecoder string32Decoder = new String32TypeDecoder();
    private final Symbol8TypeDecoder sym8Decoder = new Symbol8TypeDecoder();
    private final Symbol32TypeDecoder sym32Decoder = new Symbol32TypeDecoder();
    private final TimestampTypeDecoder timestampDecoder = new TimestampTypeDecoder();
    private final UnsignedByteTypeDecoder ubyteDecoder = new UnsignedByteTypeDecoder();
    private final UnsignedInteger0TypeDecoder uint0Decoder = new UnsignedInteger0TypeDecoder();
    private final UnsignedInteger8TypeDecoder uint8Decoder = new UnsignedInteger8TypeDecoder();
    private final UnsignedInteger32TypeDecoder uint32Decoder = new UnsignedInteger32TypeDecoder();
    private final UnsignedLong0TypeDecoder ulong0Decoder = new UnsignedLong0TypeDecoder();
    private final UnsignedLong8TypeDecoder ulong8Decoder = new UnsignedLong8TypeDecoder();
    private final UnsignedLong64TypeDecoder ulong64Decoder = new UnsignedLong64TypeDecoder();
    private final UnsignedShortTypeDecoder ushortDecoder = new UnsignedShortTypeDecoder();
    private final UUIDTypeDecoder uuidDecoder = new UUIDTypeDecoder();

    @Override
    public DecoderState newDecoderState() {
        return new ProtonDecoderState(this);
    }

    @Override
    public Object readObject(ByteBuf buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = readNextTypeDecoder(buffer, state);

        if (decoder == null) {
            throw new IOException("Unknown type constructor in encoded bytes");
        }

        if (decoder instanceof PrimitiveArrayTypeDecoder) {
            PrimitiveArrayTypeDecoder arrayDecoder = (PrimitiveArrayTypeDecoder) decoder;
            return arrayDecoder.readValueAsObject(buffer, state);
        } else {
            return decoder.readValue(buffer, state);
        }
    }

    @Override
    public TypeDecoder<?> readNextTypeDecoder(ByteBuf buffer, DecoderState state) throws IOException {
        int encodingCode = buffer.readByte() & 0xff;

        if (encodingCode == EncodingCodes.DESCRIBED_TYPE_INDICATOR) {
            Object descriptor = readObject(buffer, state);
            final TypeDecoder<?> decoder = describedTypeDecoders.get(descriptor);
            if (decoder == null) {
                throw new IllegalStateException("No registered decoder for described: " + descriptor);
            }

            return decoder;
        } else {
            if (encodingCode > primitiveDecoders.length) {
                throw new IOException("Read unknown encoding code from buffer");
            }

            return primitiveDecoders[encodingCode];
        }
    }

    @Override
    public TypeDecoder<?> peekNextTypeDecoder(ByteBuf buffer, DecoderState state) throws IOException {
        int readIndex = buffer.readerIndex();
        try {
            return readNextTypeDecoder(buffer, state);
        } finally {
            buffer.readerIndex(readIndex);
        }
    }

    @Override
    public <V> ProtonDecoder registerDescribedTypeDecoder(DescribedTypeDecoder<V> decoder) {
        describedTypeDecoders.put(decoder.getDescriptorCode(), decoder);
        describedTypeDecoders.put(decoder.getDescriptorSymbol(), decoder);
        return this;
    }

    @Override
    public <V> ProtonDecoder registerPrimitiveTypeDecoder(PrimitiveTypeDecoder<V> decoder) {
        primitiveDecoders[decoder.getTypeCode()] = decoder;
        return this;
    }

    @Override
    public TypeDecoder<?> getTypeDecoder(Object instance) {
        return null;
    }

    @Override
    public Boolean readBoolean(ByteBuf buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.BOOLEAN_TRUE:
                return trueDecoder.readValue(buffer, state);
            case EncodingCodes.BOOLEAN_FALSE:
                return falseDecoder.readValue(buffer, state);
            case EncodingCodes.BOOLEAN:
                return booleanDecoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected boolean type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Byte readByte(ByteBuf buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.BYTE:
                return byteDecoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected byte type but found encoding: " + encodingCode);
        }
    }

    @Override
    public UnsignedByte readUnsignedByte(ByteBuf buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.UBYTE:
                return ubyteDecoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected unsigned byte type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Character readCharacter(ByteBuf buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.CHAR:
                return charDecoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Character type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Decimal32 readDecimal32(ByteBuf buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.DECIMAL32:
                return decimal32Decoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Decimal32 type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Decimal64 readDecimal64(ByteBuf buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.DECIMAL64:
                return decimal64Decoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Decimal64 type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Decimal128 readDecimal128(ByteBuf buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.DECIMAL128:
                return decimal128Decoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Decimal128 type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Short readShort(ByteBuf buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.SHORT:
                return shortDecoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Short type but found encoding: " + encodingCode);
        }
    }

    @Override
    public UnsignedShort readUnsignedShort(ByteBuf buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.USHORT:
                return ushortDecoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected UnsignedShort type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Integer readInteger(ByteBuf buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.SMALLINT:
                return int8Decoder.readValue(buffer, state);
            case EncodingCodes.INT:
                return int32Decoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Integer type but found encoding: " + encodingCode);
        }
    }

    @Override
    public UnsignedInteger readUnsignedInteger(ByteBuf buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.UINT0:
                return uint0Decoder.readValue(buffer, state);
            case EncodingCodes.SMALLUINT:
                return uint8Decoder.readValue(buffer, state);
            case EncodingCodes.UINT:
                return uint32Decoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected UnsignedInteger type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Long readLong(ByteBuf buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.SMALLLONG:
                return long8Decoder.readValue(buffer, state);
            case EncodingCodes.LONG:
                return longDecoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Long type but found encoding: " + encodingCode);
        }
    }

    @Override
    public UnsignedLong readUnsignedLong(ByteBuf buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.ULONG0:
                return ulong0Decoder.readValue(buffer, state);
            case EncodingCodes.SMALLULONG:
                return ulong8Decoder.readValue(buffer, state);
            case EncodingCodes.ULONG:
                return ulong64Decoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Long type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Float readFloat(ByteBuf buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.FLOAT:
                return floatDecoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Float type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Double readDouble(ByteBuf buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.DOUBLE:
                return doubleDecoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Double type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Binary readBinary(ByteBuf buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.VBIN8:
                return binary8Decoder.readValue(buffer, state);
            case EncodingCodes.VBIN32:
                return binary32Decoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Binary type but found encoding: " + encodingCode);
        }
    }

    @Override
    public String readString(ByteBuf buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.STR8:
                return string8Decoder.readValue(buffer, state);
            case EncodingCodes.STR32:
                return string32Decoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected String type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Symbol readSymbol(ByteBuf buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.SYM8:
                return sym8Decoder.readValue(buffer, state);
            case EncodingCodes.SYM32:
                return sym32Decoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Symbol type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Long readTimestamp(ByteBuf buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.TIMESTAMP:
                return timestampDecoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Timestamp type but found encoding: " + encodingCode);
        }
    }

    @Override
    public UUID readUUID(ByteBuf buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.UUID:
                return uuidDecoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected UUID type but found encoding: " + encodingCode);
        }
    }
}
