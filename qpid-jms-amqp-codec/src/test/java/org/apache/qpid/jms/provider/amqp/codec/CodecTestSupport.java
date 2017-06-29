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
package org.apache.qpid.jms.provider.amqp.codec;

import java.io.IOException;
import java.util.Map;

import org.apache.qpid.jms.provider.amqp.codec.buffers.NettyWritableBuffer;
import org.apache.qpid.jms.provider.amqp.codec.decoders.ProtonDecoderFactory;
import org.apache.qpid.jms.provider.amqp.codec.encoders.ProtonEncoderFactory;
import org.apache.qpid.proton.codec.AMQPDefinedTypes;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.junit.Before;

import io.netty.buffer.ByteBuf;

/**
 * Support class for tests of the type decoders
 */
public class CodecTestSupport {

    protected DecoderImpl protonDecoder;
    protected EncoderImpl protonEncoder;

    protected DecoderState decoderState;
    protected EncoderState encoderState;
    protected Decoder decoder;
    protected Encoder encoder;

    private final NettyWritableBuffer wrapper = new NettyWritableBuffer(null);

    @Before
    public void setUp() {
        protonDecoder = new DecoderImpl();
        protonEncoder = new EncoderImpl(protonDecoder);
        AMQPDefinedTypes.registerAllTypes(protonDecoder, protonEncoder);

        decoder = ProtonDecoderFactory.create();
        decoderState = decoder.newDecoderState();

        encoder = ProtonEncoderFactory.create();
        encoderState = encoder.newEncoderState();
    }

    //----- Encode methods with proton / non-proton switch -------------------//

    protected ByteBuf encodeBoolean(ByteBuf buffer, boolean value, boolean useProton) {
        if (!useProton) {
            encoder.writeBoolean(buffer, encoderState, value);
        } else {
            wrapper.setBuffer(buffer);

            protonEncoder.setByteBuffer(wrapper);
            protonEncoder.writeBoolean(value);

            wrapper.setBuffer(null);
        }

        return buffer;
    }

    protected ByteBuf encodeBooleanArray(ByteBuf buffer, boolean[] value, boolean useProton) {
        if (!useProton) {
            encoder.writeArray(buffer, encoderState, value);
        } else {
            wrapper.setBuffer(buffer);

            protonEncoder.setByteBuffer(wrapper);
            protonEncoder.writeArray(value);

            wrapper.setBuffer(null);
        }

        return buffer;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected ByteBuf encodeMap(ByteBuf buffer, Map value, boolean useProton) {
        if (!useProton) {
            encoder.writeMap(buffer, encoderState, value);
        } else {
            wrapper.setBuffer(buffer);

            protonEncoder.setByteBuffer(wrapper);
            protonEncoder.writeMap(value);

            wrapper.setBuffer(null);
        }

        return buffer;
    }

    protected ByteBuf encodeString(ByteBuf buffer, String value, boolean useProton) {
        if (!useProton) {
            encoder.writeString(buffer, encoderState, value);
        } else {
            wrapper.setBuffer(buffer);

            protonEncoder.setByteBuffer(wrapper);
            protonEncoder.writeString(value);

            wrapper.setBuffer(null);
        }

        return buffer;
    }

    protected ByteBuf encodeObject(ByteBuf buffer, Object value, boolean useProton) {
        if (!useProton) {
            encoder.writeObject(buffer, encoderState, value);
        } else {
            wrapper.setBuffer(buffer);

            protonEncoder.setByteBuffer(wrapper);
            protonEncoder.writeObject(value);

            wrapper.setBuffer(null);
        }

        return buffer;
    }

    //----- Decode methods with proton / non-proton switch -------------------//

    protected Object decodeObject(ByteBuf buffer, boolean useProton) throws IOException {
        if (useProton) {
            protonDecoder.setByteBuffer(buffer.nioBuffer());
            Object result = protonDecoder.readObject();
            protonDecoder.setByteBuffer(null);
            return result;
        } else {
            return decoder.readObject(buffer, decoderState);
        }
    }
}
