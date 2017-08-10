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
package org.apache.qpid.jms.codec.decoders.primitives;

import org.apache.qpid.jms.codec.DecoderState;
import org.apache.qpid.jms.codec.decoders.primitives.BinaryTypeDecoder;
import org.apache.qpid.proton.amqp.Binary;

import io.netty.buffer.ByteBuf;

/**
 * Base class for the various Binary type decoders used to read AMQP Binary values.
 */
public abstract class AbstractBinaryTypeDecoder implements BinaryTypeDecoder {

    @Override
    public Binary readValue(ByteBuf buffer, DecoderState state) {
        int length = readSize(buffer);

        if (length > buffer.readableBytes()) {
            throw new IllegalArgumentException(
                String.format("Binary data size %d is specified to be greater than the amount " +
                              "of data available (%d)", length, buffer.readableBytes()));
        }

        // TODO - If we can plug in the memory allocator we could pool these bytes
        byte[] data = new byte[length];
        buffer.readBytes(data, 0, length);

        return new Binary(data);
    }

    protected abstract int readSize(ByteBuf buffer);

}
