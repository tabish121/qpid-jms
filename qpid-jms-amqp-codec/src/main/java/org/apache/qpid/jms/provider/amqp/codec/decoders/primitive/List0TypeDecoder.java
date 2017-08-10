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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.qpid.jms.provider.amqp.codec.DecoderState;
import org.apache.qpid.jms.provider.amqp.codec.EncodingCodes;

import io.netty.buffer.ByteBuf;

/**
 * Decoder of Zero sized AMQP List values from a byte stream.
 */
@SuppressWarnings( "unchecked" )
public class List0TypeDecoder implements ListTypeDecoder {

    @Override
    public List<Object> readValue(ByteBuf buffer, DecoderState state) {
        return Collections.EMPTY_LIST;
    }

    @Override
    public int getTypeCode() {
        return EncodingCodes.LIST0 & 0xff;
    }

    @Override
    public void readValue(ByteBuf buffer, DecoderState state, ListEntryHandler handler, Object target) throws IOException {
        // Nothing to do here, no entries in the list.
    }
}
