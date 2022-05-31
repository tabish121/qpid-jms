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
import io.netty5.buffer.api.ByteCursor;

/**
 * ReadableBuffer implementation that wraps a Netty ByteBuf
 */
public class AmqpReadableBuffer implements ReadableBuffer {

    private static final int DEFAULT_MARK = -1;

    private int markedReadIndex = DEFAULT_MARK;
    private Buffer buffer;

    public AmqpReadableBuffer(Buffer buffer) {
        this.buffer = buffer;
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
        return false; // TODO: Buffer API abstracts away low level bytes source
    }

    @Override
    public byte[] array() {
        return null; // TODO: Buffer API abstracts away low level bytes source
    }

    @Override
    public int arrayOffset() {
        return 0; // TODO: Buffer API abstracts away low level bytes source
    }

    @Override
    public ReadableBuffer reclaimRead() {
        return this;
    }

    @Override
    public byte get() {
        return buffer.readByte();
    }

    @Override
    public byte get(int index) {
        return buffer.getByte(index);
    }

    @Override
    public int getInt() {
        return buffer.readInt();
    }

    @Override
    public long getLong() {
        return buffer.readLong();
    }

    @Override
    public short getShort() {
        return buffer.readShort();
    }

    @Override
    public float getFloat() {
        return buffer.readFloat();
    }

    @Override
    public double getDouble() {
        return buffer.readDouble();
    }

    @Override
    public ReadableBuffer get(byte[] target, int offset, int length) {
        buffer.readBytes(target, offset, length);
        return this;
    }

    @Override
    public ReadableBuffer get(byte[] target) {
        buffer.readBytes(target, 0, target.length);
        return this;
    }

    @Override
    public ReadableBuffer get(WritableBuffer target) {
        int start = target.position();

        // TODO: Might be faster to use foreachReadable
        ByteCursor cursor = buffer.openCursor();
        while (cursor.readByte()) {
            target.put(cursor.getByte());
        }

        int written = target.position() - start;

        buffer.readerOffset(buffer.readerOffset() + written);

        return this;
    }

    @Override
    public ReadableBuffer slice() {
        return new AmqpReadableBuffer(buffer.slice());
    }

    @Override
    public ReadableBuffer flip() {
        buffer.writerOffset(buffer.readerOffset());
        buffer.readerOffset(0);
        return this;
    }

    @Override
    public ReadableBuffer limit(int limit) {
        buffer.writerOffset(limit);
        return this;
    }

    @Override
    public int limit() {
        return buffer.writerOffset();
    }

    @Override
    public ReadableBuffer position(int position) {
        buffer.readerOffset(position);
        return this;
    }

    @Override
    public int position() {
        return buffer.readerOffset();
    }

    @Override
    public ReadableBuffer mark() {
        markedReadIndex = buffer.readerOffset();
        return this;
    }

    @Override
    public ReadableBuffer reset() {
        if (markedReadIndex != DEFAULT_MARK) {
            buffer.readerOffset(markedReadIndex);
            markedReadIndex = DEFAULT_MARK;
        }
        return this;
    }

    @Override
    public ReadableBuffer rewind() {
        buffer.readerOffset(0);
        return this;
    }

    @Override
    public ReadableBuffer clear() {
        buffer.writerOffset(buffer.capacity());
        buffer.readerOffset(0);

        return this;
    }

    @Override
    public int remaining() {
        return buffer.readableBytes();
    }

    @Override
    public boolean hasRemaining() {
        return buffer.readableBytes() > 0;
    }

    @Override
    public ReadableBuffer duplicate() {
        return new AmqpReadableBuffer(buffer.copy());
    }

    @Override
    public ByteBuffer byteBuffer() {
        byte[] copy = new byte[buffer.readableBytes()];
        buffer.copyInto(buffer.readerOffset(), copy, 0, copy.length);
        return ByteBuffer.wrap(copy);
    }

    @Override
    public String readUTF8() throws CharacterCodingException {
        return buffer.toString(StandardCharsets.UTF_8);
    }

    @Override
    public String readString(CharsetDecoder decoder) throws CharacterCodingException {
        return buffer.toString(StandardCharsets.UTF_8);
    }
}
