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
import java.nio.charset.StandardCharsets;

import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;

/**
 * Writable Buffer implementation based on a Netty ByteBuf
 */
public class AmqpWritableBuffer implements WritableBuffer {

    public static final int INITIAL_CAPACITY = 1024;

    public Buffer nettyBuffer;

    public AmqpWritableBuffer() {
        nettyBuffer = BufferAllocator.onHeapUnpooled().allocate(INITIAL_CAPACITY);
    }

    public AmqpWritableBuffer(Buffer buffer) {
        nettyBuffer = buffer;
    }

    public Buffer getBuffer() {
        return nettyBuffer;
    }

    @Override
    public void put(byte b) {
        nettyBuffer.writeByte(b);
    }

    @Override
    public void putFloat(float f) {
        nettyBuffer.writeFloat(f);
    }

    @Override
    public void putDouble(double d) {
        nettyBuffer.writeDouble(d);
    }

    @Override
    public void put(byte[] src, int offset, int length) {
        nettyBuffer.writeBytes(src, offset, length);
    }

    @Override
    public void put(ByteBuffer payload) {
        nettyBuffer.writeBytes(payload);
    }

    public void put(Buffer payload) {
        nettyBuffer.writeBytes(payload);
    }

    @Override
    public void putShort(short s) {
        nettyBuffer.writeShort(s);
    }

    @Override
    public void putInt(int i) {
        nettyBuffer.writeInt(i);
    }

    @Override
    public void putLong(long l) {
        nettyBuffer.writeLong(l);
    }

    @Override
    public void put(String value) {
        nettyBuffer.writeCharSequence(value, StandardCharsets.UTF_8);
    }

    @Override
    public boolean hasRemaining() {
        return nettyBuffer.writerOffset() < nettyBuffer.capacity();
    }

    @Override
    public int remaining() {
        return nettyBuffer.capacity() - nettyBuffer.writerOffset();
    }

    @Override
    public void ensureRemaining(int remaining) {
        nettyBuffer.ensureWritable(remaining);
    }

    @Override
    public int position() {
        return nettyBuffer.writerOffset();
    }

    @Override
    public void position(int position) {
        nettyBuffer.writerOffset(position);
    }

    @Override
    public int limit() {
        return nettyBuffer.capacity();
    }

    @Override
    public void put(ReadableBuffer buffer) {
        buffer.get(this);
    }
}
