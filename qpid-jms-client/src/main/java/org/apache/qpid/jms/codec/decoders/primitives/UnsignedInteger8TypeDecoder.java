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
import org.apache.qpid.jms.codec.EncodingCodes;
import org.apache.qpid.jms.codec.decoders.primitives.UnsignedInteger32TypeDecoder;
import org.apache.qpid.proton.amqp.UnsignedInteger;

import io.netty.buffer.ByteBuf;

/**
 * Decode AMQP Small Unsigned Integer values from a byte stream
 */
public class UnsignedInteger8TypeDecoder extends UnsignedInteger32TypeDecoder {

    @Override
    public int getTypeCode() {
        return EncodingCodes.SMALLUINT & 0xff;
    }

    @Override
    public UnsignedInteger readValue(ByteBuf buffer, DecoderState state) {
        return UnsignedInteger.valueOf((buffer.readByte()) & 0xff);
    }
}
