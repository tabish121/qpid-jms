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
package org.apache.qpid.jms.provider.amqp.message;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;

import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;

import io.netty5.buffer.api.Buffer;

/**
 * ReadableBuffer implementation that wraps a Netty ByteBuf, the wrapped buffer
 * is copied as a read-only buffer meaning that it maintains a separate read offset
 * marker from that of the buffer used to create this instance.
 */
public class AmqpReadableBuffer implements ReadableBuffer {

    private static final int NOT_SET = -1;

    private Buffer buffer;

    private int markIndex = NOT_SET;
    private int readLimit;

    public AmqpReadableBuffer(Buffer buffer) {
        this.buffer = buffer.copy(true);
        this.readLimit = buffer.writerOffset();
    }

    AmqpReadableBuffer(Buffer buffer, int readLimit) {
        this.buffer = buffer;
        this.readLimit = buffer.writerOffset();
    }

    public Buffer getBuffer() {
        return buffer;
    }

    @Override
    public int capacity() {
        return buffer.capacity();
    }

    @Override
    public boolean hasArray() {
        return false;
    }

    @Override
    public byte[] array() {
        return null;
    }

    @Override
    public int arrayOffset() {
        return -1;
    }

    @Override
    public ReadableBuffer reclaimRead() {
        return this;
    }

    @Override
    public byte get() {
        if (buffer.readerOffset() + Byte.BYTES > readLimit) {
            throw new IndexOutOfBoundsException("Cannot read past buffer read limit: " + readLimit);
        }
        return buffer.readByte();
    }

    @Override
    public byte get(int index) {
        return buffer.getByte(index);
    }

    @Override
    public int getInt() {
        if (buffer.readerOffset() + Integer.BYTES > readLimit) {
            throw new IndexOutOfBoundsException("Cannot read past buffer read limit: " + readLimit);
        }

        return buffer.readInt();
    }

    @Override
    public long getLong() {
        if (buffer.readerOffset() + Long.BYTES > readLimit) {
            throw new IndexOutOfBoundsException("Cannot read past buffer read limit: " + readLimit);
        }

        return buffer.readLong();
    }

    @Override
    public short getShort() {
        if (buffer.readerOffset() + Short.BYTES > readLimit) {
            throw new IndexOutOfBoundsException("Cannot read past buffer read limit: " + readLimit);
        }

        return buffer.readShort();
    }

    @Override
    public float getFloat() {
        if (buffer.readerOffset() + Float.BYTES > readLimit) {
            throw new IndexOutOfBoundsException("Cannot read past buffer read limit: " + readLimit);
        }

        return buffer.readFloat();
    }

    @Override
    public double getDouble() {
        if (buffer.readerOffset() + Double.BYTES > readLimit) {
            throw new IndexOutOfBoundsException("Cannot read past buffer read limit: " + readLimit);
        }

        return buffer.readDouble();
    }

    @Override
    public ReadableBuffer get(byte[] target, int offset, int length) {
        if (buffer.readerOffset() + length > readLimit) {
            throw new IndexOutOfBoundsException("Cannot read past buffer read limit: " + readLimit);
        }

        buffer.readBytes(target, offset, length);
        return this;
    }

    @Override
    public ReadableBuffer get(byte[] target) {
        if (buffer.readerOffset() + target.length > readLimit) {
            throw new IndexOutOfBoundsException("Cannot read past buffer read limit: " + readLimit);
        }

        buffer.copyInto(buffer.readerOffset(), target, 0, target.length);
        buffer.skipReadableBytes(target.length);
        return this;
    }

    @Override
    public ReadableBuffer get(WritableBuffer target) {
        if (target instanceof AmqpWritableBuffer) {
            AmqpWritableBuffer targetWB = (AmqpWritableBuffer) target;
            targetWB.getBuffer().writeBytes(buffer);
        } else {
            while (buffer.readableBytes() > 0 && buffer.readerOffset() < readLimit) {
                target.put(buffer.readByte());
            }
        }

        return this;
    }

    @Override
    public ReadableBuffer slice() {
        return new AmqpReadableBuffer(buffer.copy(buffer.readerOffset(), readLimit - buffer.readerOffset(), true));
    }

    @Override
    public ReadableBuffer flip() {
        readLimit = buffer.readerOffset();
        buffer.readerOffset(0);
        return this;
    }

    @Override
    public ReadableBuffer limit(int limit) {
        if (limit > buffer.capacity() | limit < 0) {
            throw new IllegalArgumentException("Provided limit is invalid: " + limit);
        }
        this.readLimit = limit;

        if (buffer.readerOffset() > limit) {
            buffer.readerOffset(limit);
        }
        if (markIndex > limit) {
            markIndex = NOT_SET;
        }

        readLimit = limit;
        return this;
    }

    @Override
    public int limit() {
        return readLimit;
    }

    @Override
    public ReadableBuffer position(int position) {
        if (position > readLimit | position < 0) {
            throw new IndexOutOfBoundsException("Cannot set position of buffer past the read limit: " + readLimit);
        }

        if (markIndex > position) {
            markIndex = NOT_SET;
        }

        buffer.readerOffset(position);
        return this;
    }

    @Override
    public int position() {
        return buffer.readerOffset();
    }

    @Override
    public ReadableBuffer mark() {
        markIndex = buffer.readerOffset();
        return this;
    }

    @Override
    public ReadableBuffer reset() {
        if (markIndex != NOT_SET) {
            buffer.readerOffset(markIndex);
        }
        return this;
    }

    @Override
    public ReadableBuffer rewind() {
        buffer.readerOffset(0);
        markIndex = NOT_SET;
        return this;
    }

    @Override
    public ReadableBuffer clear() {
        buffer.readerOffset(0);
        readLimit = buffer.writerOffset();
        markIndex = NOT_SET;
        return this;
    }

    @Override
    public int remaining() {
        return readLimit - buffer.readerOffset();
    }

    @Override
    public boolean hasRemaining() {
        return readLimit - buffer.readerOffset() > 0;
    }

    @Override
    public ReadableBuffer duplicate() {
        return new AmqpReadableBuffer(buffer.copy(true), readLimit);
    }

    @Override
    public ByteBuffer byteBuffer() {
        ByteBuffer copy = ByteBuffer.allocate(buffer.readableBytes());

        buffer.copyInto(buffer.readerOffset(), copy, 0, copy.capacity());

        return copy;
    }

    @Override
    public String readUTF8() throws CharacterCodingException {
        return buffer.readCharSequence(readLimit - buffer.readerOffset(), StandardCharsets.UTF_8).toString();
    }

    @Override
    public String readString(CharsetDecoder decoder) throws CharacterCodingException {
        return buffer.readCharSequence(readLimit - buffer.readerOffset(), decoder.charset()).toString();
    }
}
