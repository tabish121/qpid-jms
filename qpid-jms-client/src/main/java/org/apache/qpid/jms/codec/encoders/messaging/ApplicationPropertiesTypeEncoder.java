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
package org.apache.qpid.jms.codec.encoders.messaging;

import java.util.Map.Entry;
import java.util.Set;

import org.apache.qpid.jms.codec.DescribedMapTypeEncoder;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;

/**
 * Encoder of AMQP ApplicationProperties type values to a byte stream.
 */
public class ApplicationPropertiesTypeEncoder implements DescribedMapTypeEncoder<String, Object, ApplicationProperties> {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000074L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:application-properties:map");

    @Override
    public Class<ApplicationProperties> getTypeClass() {
        return ApplicationProperties.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return DESCRIPTOR_SYMBOL;
    }

    @Override
    public boolean hasMap(ApplicationProperties value) {
        return value.getValue() != null;
    }

    @Override
    public int getMapSize(ApplicationProperties value) {
        if (value.getValue() != null) {
            return value.getValue().size();
        } else {
            return 0;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<Entry<String, Object>> getMapEntries(ApplicationProperties value) {
        if (value.getValue() != null) {
            return value.getValue().entrySet();
        } else {
            return null;
        }
    }
}
