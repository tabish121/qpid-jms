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
package org.apache.qpid.jms.integration;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.ServerSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;

import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for expected behaviors of JMS Connection Consumer implementation.
 */
public class ConnectionConsumerIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionConsumerIntegrationTest.class);

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    @Test
    public void testCreateConnectionConsumer() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            JmsServerSessionPool sessionPool = new JmsServerSessionPool();
            Connection connection = testFixture.establishConnecton(testPeer);

            LOG.info("Creating new ConnectionConsumer");

            // No additional Begin calls as there's no Session created for a Connection Consumer
            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlow();

            Queue queue = new JmsQueue("myQueue");
            ConnectionConsumer consumer = connection.createConnectionConsumer(queue, null, sessionPool, 100);

            testPeer.expectDetach(true, true, true);
            consumer.close();

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    //----- Internal ServerSessionPool ---------------------------------------//

    @SuppressWarnings("unused")
    private class JmsServerSessionPool implements ServerSessionPool {

        private JmsServerSession serverSession;

        public JmsServerSessionPool() {
            this.serverSession = new JmsServerSession();
        }

        public JmsServerSessionPool(JmsServerSession serverSession) {
            this.serverSession = serverSession;
        }

        @Override
        public ServerSession getServerSession() throws JMSException {
            return serverSession;
        }
    }

    @SuppressWarnings("unused")
    private class JmsServerSession implements ServerSession {

        private Session session;

        public JmsServerSession() {
        }

        public JmsServerSession(Session session) {
            this.session = session;
        }

        @Override
        public Session getSession() throws JMSException {
            return session;
        }

        @Override
        public void start() throws JMSException {
            session.run();
        }
    }
}
