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
package org.apache.qpid.jms.provider.amqp.codec.encoders.messaging;

import java.util.Map.Entry;
import java.util.Set;

import org.apache.qpid.jms.provider.amqp.codec.DescribedMapTypeEncoder;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.Footer;

/**
 * Encoder of AMQP Footer type values to a byte stream
 */
public class FooterTypeEncoder implements DescribedMapTypeEncoder<Object, Object, Footer> {

    private static final UnsignedLong descriptorCode = UnsignedLong.valueOf(0x0000000000000078L);
    private static final Symbol descriptorSymbol = Symbol.valueOf("amqp:footer:map");

    @Override
    public Class<Footer> getTypeClass() {
        return Footer.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return descriptorCode;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return descriptorSymbol;
    }

    @Override
    public boolean hasMap(Footer value) {
        return value.getValue() != null;
    }

    @Override
    public int getMapSize(Footer value) {
        if (value.getValue() != null) {
            return value.getValue().size();
        } else {
            return 0;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<Entry<Object, Object>> getMapEntries(Footer value) {
        if (value.getValue() != null) {
            return value.getValue().entrySet();
        } else {
            return null;
        }
    }
}
