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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;

/**
 * Writable buffer that uses a {@link Deflater} to compress the bytes written
 * into an {@link ByteBuf}.
 */
public class AmqpDeflatingWritableBuffer implements WritableBuffer {

    public static final int INITIAL_CAPACITY = 1024;

    public final ByteBuf nettyBuffer;
    public final DeflaterOutputStream output;
    public final DataOutputStream dataOut;

    public AmqpDeflatingWritableBuffer() {
        this(Unpooled.buffer(INITIAL_CAPACITY));
    }

    public AmqpDeflatingWritableBuffer(ByteBuf buffer) {
        nettyBuffer = buffer;
        output = new DeflaterOutputStream(new ByteBufOutputStream(buffer));
        dataOut = new DataOutputStream(output);
    }

    public ByteBuf finish() {
        try {
            dataOut.close();
            return nettyBuffer;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void put(byte b) {
        try {
            dataOut.write(b);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void putShort(short s) {
        try {
            dataOut.writeShort(s);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void putInt(int i) {
        try {
            dataOut.writeInt(i);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void putLong(long l) {
        try {
            dataOut.writeLong(l);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void putFloat(float f) {
        try {
            dataOut.writeFloat(f);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void putDouble(double d) {
        try {
            dataOut.writeDouble(d);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void put(byte[] src, int offset, int length) {
        try {
            dataOut.write(src, offset, length);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void put(ByteBuffer payload) {
        if (payload.hasArray()) {
            put(payload.array(), payload.arrayOffset() + payload.position(), payload.remaining());
        } else {
            final int remaining = payload.remaining();
            final int longCount = remaining >>> 3;
            final int byteCount = remaining & 7;

            for (int i = longCount; i > 0; i --) {
                putLong(payload.getLong());
            }

            for (int i = byteCount; i > 0; i --) {
                put(payload.get());
            }
        }
    }

    public void put(ByteBuf payload) {
        put(payload.nioBuffer());
    }

    @Override
    public void put(ReadableBuffer payload) {
        if (payload.hasArray()) {
            put(payload.array(), payload.arrayOffset() + payload.position(), payload.remaining());
        } else {
            final int remaining = payload.remaining();
            final int longCount = remaining >>> 3;
            final int byteCount = remaining & 7;

            for (int i = longCount; i > 0; i --) {
                putLong(payload.getLong());
            }

            for (int i = byteCount; i > 0; i --) {
                put(payload.get());
            }
        }
    }

    @Override
    public boolean hasRemaining() {
        return nettyBuffer.writerIndex() < nettyBuffer.maxCapacity();
    }

    @Override
    public int remaining() {
        return nettyBuffer.maxCapacity() - nettyBuffer.writerIndex();
    }

    @Override
    public void ensureRemaining(int remaining) {
        nettyBuffer.ensureWritable(remaining);
    }

    @Override
    public int position() {
        return nettyBuffer.writerIndex();
    }

    @Override
    public void position(int position) {
        nettyBuffer.writerIndex(position);
    }

    @Override
    public int limit() {
        return nettyBuffer.capacity();
    }
}
