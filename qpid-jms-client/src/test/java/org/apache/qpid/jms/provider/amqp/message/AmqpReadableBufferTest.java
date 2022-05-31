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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;

import org.apache.qpid.proton.codec.ReadableBuffer;
import org.junit.Test;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;

/**
 * Tests for the ReadableBuffer wrapper that uses Netty ByteBuf underneath
 */
public class AmqpReadableBufferTest {

    @Test
    public void testWrapBuffer() {
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().allocate(100).implicitCapacityLimit(100);

        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        assertEquals(100, buffer.capacity());
        assertSame(byteBuffer, buffer.getBuffer());
        assertSame(buffer, buffer.reclaimRead());
    }

    @Test
    public void testArrayAccess() {
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().allocate(100).implicitCapacityLimit(100);
        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        assertFalse(buffer.hasArray());
    }

    @Test
    public void testArrayAccessWhenNoArray() {
        Buffer byteBuffer = BufferAllocator.offHeapUnpooled().allocate(10);
        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        assertFalse(buffer.hasArray());
    }

    @Test
    public void testByteBuffer() {
        byte[] data = new byte[] { 0, 1, 2, 3, 4 };
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().copyOf(data);
        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        ByteBuffer nioBuffer = buffer.byteBuffer();
        assertEquals(data.length, nioBuffer.remaining());

        for (int i = 0; i < data.length; i++) {
            assertEquals(data[i], nioBuffer.get());
        }
    }

    @Test
    public void testGet() {
        byte[] data = new byte[] { 0, 1, 2, 3, 4 };
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().copyOf(data);
        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        for (int i = 0; i < data.length; i++) {
            assertEquals(data[i], buffer.get());
        }

        assertFalse(buffer.hasRemaining());

        try {
            buffer.get();
            fail("Should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}
    }

    @Test
    public void testGetIndex() {
        byte[] data = new byte[] { 0, 1, 2, 3, 4 };
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().copyOf(data);
        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        for (int i = 0; i < data.length; i++) {
            assertEquals(data[i], buffer.get(i));
        }

        assertTrue(buffer.hasRemaining());
    }

    @Test
    public void testGetShort() {
        byte[] data = new byte[] { 0, 1 };
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().copyOf(data);
        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        assertEquals(1, buffer.getShort());
        assertFalse(buffer.hasRemaining());

        try {
            buffer.getShort();
            fail("Should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}
    }

    @Test
    public void testGetInt() {
        byte[] data = new byte[] { 0, 0, 0, 1 };
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().copyOf(data);
        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        assertEquals(1, buffer.getInt());
        assertFalse(buffer.hasRemaining());

        try {
            buffer.getInt();
            fail("Should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}
    }

    @Test
    public void testGetLong() {
        byte[] data = new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 };
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().copyOf(data);
        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        assertEquals(1, buffer.getLong());
        assertFalse(buffer.hasRemaining());

        try {
            buffer.getLong();
            fail("Should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}
    }

    @Test
    public void testGetFloat() {
        byte[] data = new byte[] { 0, 0, 0, 0 };
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().copyOf(data);
        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        assertEquals(0, buffer.getFloat(), 0.0);
        assertFalse(buffer.hasRemaining());

        try {
            buffer.getFloat();
            fail("Should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}
    }

    @Test
    public void testGetDouble() {
        byte[] data = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 };
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().copyOf(data);
        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        assertEquals(0, buffer.getDouble(), 0.0);
        assertFalse(buffer.hasRemaining());

        try {
            buffer.getDouble();
            fail("Should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}
    }

    @Test
    public void testGetBytes() {
        byte[] data = new byte[] { 0, 1, 2, 3, 4};
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().copyOf(data);
        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        byte[] target = new byte[data.length];

        buffer.get(target);
        assertFalse(buffer.hasRemaining());
        assertArrayEquals(data, target);

        try {
            buffer.get(target);
            fail("Should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}
    }

    @Test
    public void testGetBytesIntInt() {
        byte[] data = new byte[] { 0, 1, 2, 3, 4};
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().copyOf(data);
        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        byte[] target = new byte[data.length];

        buffer.get(target, 0, target.length);
        assertFalse(buffer.hasRemaining());
        assertArrayEquals(data, target);

        try {
            buffer.get(target, 0, target.length);
            fail("Should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}
    }

    @Test
    public void testGetBytesToWritableBuffer() {
        byte[] data = new byte[] { 0, 1, 2, 3, 4};
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().copyOf(data);
        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);
        Buffer targetBuffer = BufferAllocator.onHeapUnpooled().allocate(data.length).implicitCapacityLimit(data.length);
        AmqpWritableBuffer target = new AmqpWritableBuffer(targetBuffer);

        buffer.get(target);
        assertFalse(buffer.hasRemaining());
    }

    @Test
    public void testGetBytesToWritableBufferThatIsDirect() {
        byte[] data = new byte[] { 0, 1, 2, 3, 4};
        Buffer byteBuffer = BufferAllocator.offHeapUnpooled().copyOf(data);
        byteBuffer.writeBytes(data);
        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);
        Buffer targetBuffer = BufferAllocator.onHeapUnpooled().allocate(data.length).implicitCapacityLimit(data.length);
        AmqpWritableBuffer target = new AmqpWritableBuffer(targetBuffer);

        buffer.get(target);
        assertFalse(buffer.hasRemaining());

        for (int i = 0; i < data.length; i++) {
            assertEquals(data[i], target.getBuffer().readByte());
        }
    }

    @Test
    public void testDuplicate() {
        byte[] data = new byte[] { 0, 1, 2, 3, 4};
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().copyOf(data);
        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        ReadableBuffer duplicate = buffer.duplicate();

        for (int i = 0; i < data.length; i++) {
            assertEquals(data[i], duplicate.get());
        }

        assertFalse(duplicate.hasRemaining());
    }

    @Test
    public void testSlice() {
        byte[] data = new byte[] { 0, 1, 2, 3, 4};
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().copyOf(data);
        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        ReadableBuffer slice = buffer.slice();

        for (int i = 0; i < data.length; i++) {
            assertEquals(data[i], slice.get());
        }

        assertFalse(slice.hasRemaining());
    }

    @Test
    public void testLimit() {
        byte[] data = new byte[] { 1, 2 };
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().copyOf(data);
        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        assertEquals(data.length, buffer.limit());
        buffer.limit(1);
        assertEquals(1, buffer.limit());
        assertEquals(1, buffer.get());
        assertFalse(buffer.hasRemaining());

        try {
            buffer.get();
            fail("Should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}
    }

    @Test
    public void testClear() {
        byte[] data = new byte[] { 0, 1, 2, 3, 4};
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().copyOf(data);
        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        byte[] target = new byte[data.length];

        buffer.get(target);
        assertFalse(buffer.hasRemaining());
        assertArrayEquals(data, target);

        try {
            buffer.get(target);
            fail("Should throw an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ioe) {}

        buffer.clear();
        assertTrue(buffer.hasRemaining());
        assertEquals(data.length, buffer.remaining());
        buffer.get(target);
        assertFalse(buffer.hasRemaining());
        assertArrayEquals(data, target);
    }

    @Test
    public void testRewind() {
        byte[] data = new byte[] { 0, 1, 2, 3, 4 };
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().copyOf(data);
        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        for (int i = 0; i < data.length; i++) {
            assertEquals(data[i], buffer.get());
        }

        assertFalse(buffer.hasRemaining());
        buffer.rewind();
        assertTrue(buffer.hasRemaining());

        for (int i = 0; i < data.length; i++) {
            assertEquals(data[i], buffer.get());
        }
    }

    @Test
    public void testReset() {
        byte[] data = new byte[] { 0, 1, 2, 3, 4 };
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().copyOf(data);
        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        buffer.mark();

        for (int i = 0; i < data.length; i++) {
            assertEquals(data[i], buffer.get());
        }

        assertFalse(buffer.hasRemaining());
        buffer.reset();
        assertTrue(buffer.hasRemaining());

        for (int i = 0; i < data.length; i++) {
            assertEquals(data[i], buffer.get());
        }
    }

    @Test
    public void testGetPosition() {
        byte[] data = new byte[] { 0, 1, 2, 3, 4 };
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().copyOf(data);
        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        assertEquals(buffer.position(), 0);
        for (int i = 0; i < data.length; i++) {
            assertEquals(buffer.position(), i);
            assertEquals(data[i], buffer.get());
            assertEquals(buffer.position(), i + 1);
        }
    }

    @Test
    public void testSetPosition() {
        byte[] data = new byte[] { 0, 1, 2, 3, 4 };
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().copyOf(data);
        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        for (int i = 0; i < data.length; i++) {
            assertEquals(data[i], buffer.get());
        }

        assertFalse(buffer.hasRemaining());
        buffer.position(0);
        assertTrue(buffer.hasRemaining());

        for (int i = 0; i < data.length; i++) {
            assertEquals(data[i], buffer.get());
        }
    }

    @Test
    public void testFlip() {
        byte[] data = new byte[] { 0, 1, 2, 3, 4 };
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().copyOf(data);
        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        buffer.mark();

        for (int i = 0; i < data.length; i++) {
            assertEquals(data[i], buffer.get());
        }

        assertFalse(buffer.hasRemaining());
        buffer.flip();
        assertTrue(buffer.hasRemaining());

        for (int i = 0; i < data.length; i++) {
            assertEquals(data[i], buffer.get());
        }
    }

    @Test
    public void testReadUTF8() throws CharacterCodingException {
        String testString = "test-string-1";
        byte[] asUtf8bytes = testString.getBytes(StandardCharsets.UTF_8);
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().copyOf(asUtf8bytes);

        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        assertEquals(testString, buffer.readUTF8());
    }

    @Test
    public void testReadString() throws CharacterCodingException {
        String testString = "test-string-1";
        byte[] asUtf8bytes = testString.getBytes(StandardCharsets.UTF_8);
        Buffer byteBuffer = BufferAllocator.onHeapUnpooled().copyOf(asUtf8bytes);

        AmqpReadableBuffer buffer = new AmqpReadableBuffer(byteBuffer);

        assertEquals(testString, buffer.readString(StandardCharsets.UTF_8.newDecoder()));
    }
}
