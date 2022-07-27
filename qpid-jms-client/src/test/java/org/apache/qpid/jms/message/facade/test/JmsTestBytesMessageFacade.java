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
package org.apache.qpid.jms.message.facade.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.qpid.jms.message.facade.JmsBytesMessageFacade;

import io.netty5.buffer.BufferInputStream;
import io.netty5.buffer.BufferOutputStream;
import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;

/**
 * A test implementation of the JmsBytesMessageFacade that simply holds a raw Buffer
 */
public final class JmsTestBytesMessageFacade extends JmsTestMessageFacade implements JmsBytesMessageFacade {

    private static final Buffer EMPTY_BUFFER = BufferAllocator.onHeapUnpooled().allocate(0);

    private Buffer content = EMPTY_BUFFER;
    private BufferOutputStream bytesOut;
    private BufferInputStream bytesIn;

    public JmsTestBytesMessageFacade() {
    }

    public JmsTestBytesMessageFacade(byte[] content) {
        this.content = BufferAllocator.onHeapUnpooled().copyOf(content);
    }

    @Override
    public JmsMsgType getMsgType() {
        return JmsMsgType.BYTES;
    }

    @Override
    public JmsTestBytesMessageFacade copy() {
        reset();
        JmsTestBytesMessageFacade copy = new JmsTestBytesMessageFacade();
        copyInto(copy);
        if (this.content != null) {
            copy.content = this.content.copy();
        }

        return copy;
    }

    @Override
    public void clearBody() {
        if (bytesIn != null) {
            try {
                bytesIn.close();
            } catch (IOException e) {
            }
            bytesIn = null;
        }
        if (bytesOut != null) {
            try {
                bytesOut.close();
            } catch (IOException e) {
            }
            bytesOut = null;
        }

        content = EMPTY_BUFFER;
    }

    @Override
    public InputStream getInputStream() throws JMSException {
        if (bytesOut != null) {
            throw new IllegalStateException("Body is being written to, cannot perform a read.");
        }

        if (bytesIn == null) {
            // Duplicate the content buffer to allow for getBodyLength() validity.
            bytesIn = new BufferInputStream(content.copy().send());
        }

        return bytesIn;
    }

    @Override
    public OutputStream getOutputStream() throws JMSException {
        if (bytesIn != null) {
            throw new IllegalStateException("Body is being read from, cannot perform a write.");
        }

        if (bytesOut == null) {
            bytesOut = new BufferOutputStream(BufferAllocator.onHeapUnpooled().allocate(1));
            content = EMPTY_BUFFER;
        }

        return bytesOut;
    }

    @Override
    public void reset() {
        if (bytesOut != null) {
            content = bytesOut.buffer();
            try {
                bytesOut.close();
            } catch (IOException e) {
            }
            bytesOut = null;
        } else if (bytesIn != null) {
            try {
                bytesIn.close();
            } catch (IOException e) {
            }
            bytesIn = null;
        }
    }

    @Override
    public int getBodyLength() {
        return content.readableBytes();
    }

    @Override
    public boolean hasBody() {
        return content.readableBytes() > 0 || (bytesOut != null && bytesOut.writtenBytes() > 0);
    }

    @Override
    public byte[] copyBody() {
        byte[] result = new byte[content.readableBytes()];
        content.copyInto(content.readerOffset(), result, 0, content.readableBytes());
        return result;
    }

    @Override
    public void onSend(long producerTtl) throws JMSException {
        super.onSend(producerTtl);

        reset();
    }
}
