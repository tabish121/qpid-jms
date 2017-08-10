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
package org.apache.qpid.jms.provider.amqp.codec.decoders.primitive;

import org.apache.qpid.jms.provider.amqp.codec.DecoderState;
import org.apache.qpid.jms.provider.amqp.codec.EncodingCodes;

import io.netty.buffer.ByteBuf;

/**
 * Decode AMQP small Long values from a byte stream
 */
public class Long8TypeDecoder extends LongTypeDecoder {

    @Override
    public Long readValue(ByteBuf buffer, DecoderState state) {
        return (long) buffer.readByte() & 0xff;
    }

    @Override
    public int getTypeCode() {
        return EncodingCodes.SMALLLONG & 0xff;
    }
}
