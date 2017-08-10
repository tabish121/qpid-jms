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
package org.apache.qpid.jms.codec.decoders;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.qpid.jms.codec.DecoderState;
import org.apache.qpid.proton.amqp.Symbol;

/**
 * State object used by the Built in Decoder implementation.
 */
public class ProtonDecoderState implements DecoderState {

    private static final Map<ByteBuffer, Symbol> bufferToSymbols = new ConcurrentHashMap<>();
    private static final Symbol EMPTY_SYMBOL = Symbol.valueOf("");

    private final ProtonDecoder decoder;

    public ProtonDecoderState(ProtonDecoder decoder) {
        this.decoder = decoder;
    }

    @Override
    public ProtonDecoder getDecoder() {
        return decoder;
    }

    @Override
    public Symbol getSymbol(byte[] symbolBytes) {
        if (symbolBytes == null) {
            return null;
        } else if (symbolBytes.length == 0) {
            return EMPTY_SYMBOL;
        }

        Symbol symbol = bufferToSymbols.get(ByteBuffer.wrap(symbolBytes));

        if (symbol == null) {
            String symbolString = new String(symbolBytes, StandardCharsets.US_ASCII).intern();
            symbol = Symbol.getSymbol(symbolString);
            Symbol existing;
            if ((existing = bufferToSymbols.putIfAbsent(ByteBuffer.wrap(symbolBytes), symbol)) != null) {
                symbol = existing;
            }
        }

        return symbol;
    }
}
