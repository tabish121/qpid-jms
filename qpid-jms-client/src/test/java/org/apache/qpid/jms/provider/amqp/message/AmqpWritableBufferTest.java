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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.qpid.proton.codec.ReadableBuffer;
import org.junit.Test;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;

/**
 * Tests for behavior of AmqpWritableBuffer
 */
public class AmqpWritableBufferTest {

    @Test
    public void testGetBuffer() {
        Buffer buffer = BufferAllocator.onHeapUnpooled().allocate(1024);

        AmqpWritableBuffer writable = new AmqpWritableBuffer(buffer);

        assertSame(buffer, writable.getBuffer());
    }

    @Test
    public void testLimit() {
        Buffer buffer = BufferAllocator.onHeapUnpooled().allocate(1024);
        AmqpWritableBuffer writable = new AmqpWritableBuffer(buffer);

        assertEquals(buffer.capacity(), writable.limit());
    }

    @Test
    public void testRemaining() {
        Buffer buffer = BufferAllocator.onHeapUnpooled().allocate(1024);
        AmqpWritableBuffer writable = new AmqpWritableBuffer(buffer);

        assertEquals(buffer.capacity(), writable.remaining());
        writable.put((byte) 0);
        assertEquals(buffer.capacity() - 1, writable.remaining());
    }

    @Test
    public void testHasRemaining() {
        Buffer buffer = BufferAllocator.onHeapUnpooled().allocate(100).implicitCapacityLimit(100);
        AmqpWritableBuffer writable = new AmqpWritableBuffer(buffer);

        assertTrue(writable.hasRemaining());
        writable.put((byte) 0);
        assertTrue(writable.hasRemaining());
        buffer.writerOffset(buffer.capacity());
        assertFalse(writable.hasRemaining());
    }

    @Test
    public void testGetPosition() {
        Buffer buffer = BufferAllocator.onHeapUnpooled().allocate(1024);
        AmqpWritableBuffer writable = new AmqpWritableBuffer(buffer);

        assertEquals(0, writable.position());
        writable.put((byte) 0);
        assertEquals(1, writable.position());
    }

    @Test
    public void testSetPosition() {
        Buffer buffer = BufferAllocator.onHeapUnpooled().allocate(1024);
        AmqpWritableBuffer writable = new AmqpWritableBuffer(buffer);

        assertEquals(0, writable.position());
        writable.position(1);
        assertEquals(1, writable.position());
    }

    @Test
    public void testPutByteBuffer() {
        ByteBuffer input = ByteBuffer.allocate(1024);
        input.put((byte) 1);
        input.flip();

        Buffer buffer = BufferAllocator.onHeapUnpooled().allocate(1024);
        AmqpWritableBuffer writable = new AmqpWritableBuffer(buffer);

        assertEquals(0, writable.position());
        writable.put(input);
        assertEquals(1, writable.position());
    }

    @Test
    public void testPutByteBuf() {
        Buffer input = BufferAllocator.onHeapUnpooled().allocate(1);
        input.writeByte((byte) 1);
        input.makeReadOnly();

        Buffer buffer = BufferAllocator.onHeapUnpooled().allocate(1024);
        AmqpWritableBuffer writable = new AmqpWritableBuffer(buffer);

        assertEquals(0, writable.position());
        writable.put(input);
        assertEquals(1, writable.position());
    }

    @Test
    public void testPutString() {
        String ascii = new String("ASCII");

        Buffer buffer = BufferAllocator.onHeapUnpooled().allocate(1024);
        AmqpWritableBuffer writable = new AmqpWritableBuffer(buffer);

        assertEquals(0, writable.position());
        writable.put(ascii);
        assertEquals(ascii.length(), writable.position());
        assertEquals(ascii, writable.getBuffer().toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testPutReadableBuffer() {
        doPutReadableBufferTestImpl(true);
        doPutReadableBufferTestImpl(false);
    }

    private void doPutReadableBufferTestImpl(boolean readOnly) {
        ByteBuffer buf = ByteBuffer.allocate(1024);
        buf.put((byte) 1);
        buf.flip();
        if (readOnly) {
            buf = buf.asReadOnlyBuffer();
        }

        ReadableBuffer input = new ReadableBuffer.ByteBufferReader(buf);

        if (readOnly) {
            assertFalse("Expected buffer not to hasArray()", input.hasArray());
        } else {
            assertTrue("Expected buffer to hasArray()", input.hasArray());
        }

        Buffer buffer = BufferAllocator.onHeapUnpooled().allocate(1024);
        AmqpWritableBuffer writable = new AmqpWritableBuffer(buffer);

        assertEquals(0, writable.position());
        writable.put(input);
        assertEquals(1, writable.position());
    }
}
