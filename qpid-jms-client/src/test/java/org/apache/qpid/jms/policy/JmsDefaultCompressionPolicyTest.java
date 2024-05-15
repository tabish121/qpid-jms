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
package org.apache.qpid.jms.policy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsMessageProducer;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsSession;
import org.apache.qpid.jms.JmsTopic;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import jakarta.jms.Message;

public class JmsDefaultCompressionPolicyTest {

    @Test
    public void testDefaults() {
        JmsDestination destination = new JmsQueue("test");
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(session.isTransacted()).thenReturn(false);
        JmsMessageProducer producer = Mockito.mock(JmsMessageProducer.class);
        Message message = Mockito.mock(Message.class);

        JmsDefaultCompressionPolicy policy = new JmsDefaultCompressionPolicy();

        assertFalse(policy.isCompressAll());
        assertFalse(policy.isCompressTopicProducers());
        assertFalse(policy.isCompressQueueProducers());
        assertFalse(policy.isCompressAnonymousProducers());
        assertFalse(policy.isCompressionTarget(session, destination));
        assertTrue(policy.isCompressionTarget(producer, destination, message));
    }

    @Test
    public void testIsCompressAll() {
        JmsDestination destination = new JmsQueue("test");
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(session.isTransacted()).thenReturn(false);

        JmsDefaultCompressionPolicy policy = new JmsDefaultCompressionPolicy();
        assertFalse(policy.isCompressionTarget(session, destination));

        policy.setCompressAll(true);
        assertTrue(policy.isCompressionTarget(session, destination));
    }

    @Test
    public void testIsCompressTopicProducers() {
        JmsDestination queue = new JmsQueue("test");
        JmsDestination topic = new JmsTopic("test");
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(session.isTransacted()).thenReturn(false);

        JmsDefaultCompressionPolicy policy = new JmsDefaultCompressionPolicy();
        assertFalse(policy.isCompressionTarget(session, topic));
        assertFalse(policy.isCompressionTarget(session, queue));

        policy.setCompressTopicProducers(true);
        assertTrue(policy.isCompressionTarget(session, topic));
        assertFalse(policy.isCompressionTarget(session, queue));
    }

    @Test
    public void testIsCompressQueueProducers() {
        JmsDestination queue = new JmsQueue("test");
        JmsDestination topic = new JmsTopic("test");
        JmsSession session = Mockito.mock(JmsSession.class);
        Mockito.when(session.isTransacted()).thenReturn(false);

        JmsDefaultCompressionPolicy policy = new JmsDefaultCompressionPolicy();
        assertFalse(policy.isCompressionTarget(session, topic));
        assertFalse(policy.isCompressionTarget(session, queue));

        policy.setCompressQueueProducers(true);
        assertFalse(policy.isCompressionTarget(session, topic));
        assertTrue(policy.isCompressionTarget(session, queue));
    }

    @Test
    public void testEquals() {
        JmsDefaultCompressionPolicy policy1 = new JmsDefaultCompressionPolicy();
        JmsDefaultCompressionPolicy policy2 = new JmsDefaultCompressionPolicy();

        assertTrue(policy1.equals(policy1));
        assertTrue(policy1.equals(policy2));
        assertTrue(policy2.equals(policy1));
        assertFalse(policy1.equals(null));
        assertFalse(policy1.equals("test"));

        policy1.setCompressAll(false);
        policy1.setCompressAll(true);

        assertFalse(policy1.equals(policy2));
        assertFalse(policy2.equals(policy1));

        policy1.setCompressAll(false);
        policy1.setCompressAnonymousProducers(true);

        assertFalse(policy2.equals(policy1));

        policy1.setCompressAll(false);
        policy1.setCompressQueueProducers(false);
        policy2.setCompressTopicProducers(true);

        assertFalse(policy2.equals(policy1));

        policy1.setCompressAll(false);
        policy2.setCompressTopicProducers(true);

        assertFalse(policy2.equals(policy1));

        policy1.setCompressAll(false);
        policy2.setCompressTopicProducers(false);
        policy2.setCompressQueueProducers(true);

        assertFalse(policy2.equals(policy1));

        policy1.setCompressAll(false);
        policy2.setCompressTopicProducers(false);
        policy2.setCompressQueueProducers(false);
        policy1.setCompressTopicProducers(true);

        assertFalse(policy2.equals(policy1));

        policy1.setCompressAll(false);
        policy2.setCompressTopicProducers(false);
        policy2.setCompressQueueProducers(false);
        policy1.setCompressTopicProducers(false);
        policy1.setCompressQueueProducers(true);

        assertFalse(policy2.equals(policy1));

        policy1.setCompressAll(false);
        policy2.setCompressTopicProducers(false);
        policy2.setCompressQueueProducers(false);
        policy1.setCompressTopicProducers(false);
        policy1.setCompressQueueProducers(false);
        policy1.setCompressAnonymousProducers(true);

        assertFalse(policy2.equals(policy1));
    }

    @Test
    public void testHashCode() {
        JmsDefaultCompressionPolicy policy1 = new JmsDefaultCompressionPolicy();
        JmsDefaultCompressionPolicy policy2 = new JmsDefaultCompressionPolicy();

        assertEquals(policy1.hashCode(), policy2.hashCode());

        policy1.setCompressAll(false);
        policy2.setCompressAll(true);

        assertFalse(policy1.hashCode() == policy2.hashCode());

        policy1.setCompressAll(false);
        policy1.setCompressAnonymousProducers(true);

        assertFalse(policy1.hashCode() == policy2.hashCode());

        policy1.setCompressAll(false);
        policy1.setCompressAnonymousProducers(false);
        policy2.setCompressTopicProducers(true);

        assertFalse(policy1.hashCode() == policy2.hashCode());

        policy1.setCompressAll(false);
        policy2.setCompressTopicProducers(false);
        policy2.setCompressQueueProducers(true);

        assertFalse(policy1.hashCode() == policy2.hashCode());

        policy1.setCompressAll(false);
        policy2.setCompressTopicProducers(false);
        policy2.setCompressQueueProducers(false);
        policy1.setCompressTopicProducers(true);

        assertFalse(policy1.hashCode() == policy2.hashCode());

        policy1.setCompressAll(false);
        policy2.setCompressTopicProducers(false);
        policy2.setCompressQueueProducers(false);
        policy1.setCompressTopicProducers(false);
        policy1.setCompressQueueProducers(true);

        assertFalse(policy1.hashCode() == policy2.hashCode());

        policy1.setCompressAll(false);
        policy2.setCompressTopicProducers(false);
        policy2.setCompressQueueProducers(false);
        policy1.setCompressTopicProducers(false);
        policy1.setCompressQueueProducers(false);
        policy1.setCompressAnonymousProducers(true);

        assertFalse(policy1.hashCode() == policy2.hashCode());
    }
}
