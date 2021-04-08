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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.junit.Test;

public class AmqpMessageSupportTest {

    private static final Symbol TEST_SYMBOL = Symbol.valueOf("x-opt-test");

    @Test
    public void testCreate() {
        new AmqpMessageSupport();
    }

    //---------- getMessageAnnotation ----------------------------------------//

    @Test
    public void testGetMessageAnnotationWhenMessageHasAnnotationsMap() {
        Map<Symbol, Object> messageAnnotationsMap = new HashMap<Symbol,Object>();
        messageAnnotationsMap.put(TEST_SYMBOL, Boolean.TRUE);
        assertNotNull(AmqpMessageSupport.getMessageAnnotation(TEST_SYMBOL, new MessageAnnotations(messageAnnotationsMap)));
    }

    @Test
    public void testGetMessageAnnotationWhenMessageHasEmptyAnnotationsMap() {
        Map<Symbol, Object> messageAnnotationsMap = new HashMap<Symbol,Object>();
        assertNull(AmqpMessageSupport.getMessageAnnotation(TEST_SYMBOL, new MessageAnnotations(messageAnnotationsMap)));
    }

    @Test
    public void testGetMessageAnnotationWhenMessageAnnotationHasNoAnnotationsMap() {
        assertNull(AmqpMessageSupport.getMessageAnnotation(TEST_SYMBOL, new MessageAnnotations(null)));
    }

    @Test
    public void testGetMessageAnnotationWhenMessageIsNull() {
        assertNull(AmqpMessageSupport.getMessageAnnotation(TEST_SYMBOL, null));
    }

    //---------- isContentType -----------------------------------------------//

    @Test
    public void testIsContentTypeWithNullStringValueAndNullContentType() {
        assertTrue(AmqpMessageSupport.isContentType(null, null));
    }

    @Test
    public void testIsContentTypeWithNonNullSymbolValueAndNullContentType() {
        assertFalse(AmqpMessageSupport.isContentType("test", null));
    }

    @Test
    public void testIsContentTypeWithNonNullSymbolValueAndNonNullContentTypeNotEqual() {
        assertFalse(AmqpMessageSupport.isContentType("test", "fails"));
    }

    @Test
    public void testIsContentTypeWithNonNullSymbolValueAndNonNullContentTypeEqual() {
        assertTrue(AmqpMessageSupport.isContentType("test", "test"));
    }

    @Test
    public void testIsContentTypeWithNullSymbolValueAndNonNullContentType() {
        assertFalse(AmqpMessageSupport.isContentType(null, "test"));
    }
}
