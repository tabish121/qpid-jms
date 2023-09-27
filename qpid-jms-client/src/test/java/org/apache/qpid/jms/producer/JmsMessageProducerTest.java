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
package org.apache.qpid.jms.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.CompletionListener;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsConnectionTestSupport;
import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsMessageProducer;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsSession;
import org.apache.qpid.jms.JmsTopic;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.mock.MockRemotePeer;
import org.apache.qpid.jms.test.Wait;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test basic functionality around JmsConnection
 */
public class JmsMessageProducerTest extends JmsConnectionTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(JmsMessageProducerTest.class);

    private final MyCompletionListener completionListener = new MyCompletionListener();
    private JmsSession session;
    private final MockRemotePeer remotePeer = new MockRemotePeer();

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        remotePeer.start();
        connection = createConnectionToMockProvider();
        session = (JmsSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    @AfterEach
    public void tearDown() throws Exception {
        try {
            remotePeer.terminate();
        } finally {
            super.tearDown();
        }
    }

    @Test
    @Timeout(10)
    public void testMultipleCloseCallsNoErrors() throws Exception {
        MessageProducer producer = session.createProducer(null);
        producer.close();
        producer.close();
    }

    @Test
    @Timeout(10)
    public void testCreateProducerWithNullDestination() throws Exception {
        MessageProducer producer = session.createProducer(null);
        assertNull(producer.getDestination());
    }

    @Test
    @Timeout(10)
    public void testGetDisableMessageID() throws Exception {
        MessageProducer producer = session.createProducer(null);
        assertFalse(producer.getDisableMessageID());
        producer.setDisableMessageID(true);
        assertTrue(producer.getDisableMessageID());
    }

    @Test
    @Timeout(10)
    public void testGetDisableTimeStamp() throws Exception {
        MessageProducer producer = session.createProducer(null);
        assertFalse(producer.getDisableMessageTimestamp());
        producer.setDisableMessageTimestamp(true);
        assertTrue(producer.getDisableMessageTimestamp());
    }

    @Test
    @Timeout(10)
    public void testPriorityConfiguration() throws Exception {
        MessageProducer producer = session.createProducer(null);
        assertEquals(Message.DEFAULT_PRIORITY, producer.getPriority());
        producer.setPriority(9);
        assertEquals(9, producer.getPriority());
    }

    @Test
    @Timeout(10)
    public void testPriorityConfigurationWithInvalidPriorityValues() throws Exception {
        MessageProducer producer = session.createProducer(null);
        assertEquals(Message.DEFAULT_PRIORITY, producer.getPriority());
        try {
            producer.setPriority(-1);
            fail("Should have thrown an exception");
        } catch (JMSException ex) {
            LOG.debug("Caught expected exception: {}", ex.getMessage());
        }
        try {
            producer.setPriority(10);
            fail("Should have thrown an exception");
        } catch (JMSException ex) {
            LOG.debug("Caught expected exception: {}", ex.getMessage());
        }
        assertEquals(Message.DEFAULT_PRIORITY, producer.getPriority());
    }

    @Test
    @Timeout(10)
    public void testTimeToLiveConfiguration() throws Exception {
        MessageProducer producer = session.createProducer(null);
        assertEquals(Message.DEFAULT_TIME_TO_LIVE, producer.getTimeToLive());
        producer.setTimeToLive(1000);
        assertEquals(1000, producer.getTimeToLive());
    }

    @Test
    @Timeout(10)
    public void testDeliveryModeConfiguration() throws Exception {
        MessageProducer producer = session.createProducer(null);
        assertEquals(Message.DEFAULT_DELIVERY_MODE, producer.getDeliveryMode());
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        assertEquals(DeliveryMode.NON_PERSISTENT, producer.getDeliveryMode());
    }

    @Test
    @Timeout(10)
    public void testDeliveryModeConfigurationWithInvalidMode() throws Exception {
        MessageProducer producer = session.createProducer(null);
        assertEquals(Message.DEFAULT_DELIVERY_MODE, producer.getDeliveryMode());
        try {
            producer.setDeliveryMode(-1);
            fail("Should have thrown an exception");
        } catch (JMSException ex) {
            LOG.debug("Caught expected exception: {}", ex.getMessage());
        }
        try {
            producer.setDeliveryMode(5);
            fail("Should have thrown an exception");
        } catch (JMSException ex) {
            LOG.debug("Caught expected exception: {}", ex.getMessage());
        }
        assertEquals(Message.DEFAULT_DELIVERY_MODE, producer.getDeliveryMode());
    }

    @Test
    @Timeout(10)
    public void testDeliveryDelayConfiguration() throws Exception {
        MessageProducer producer = session.createProducer(null);
        assertEquals(Message.DEFAULT_DELIVERY_DELAY, producer.getDeliveryDelay());
        producer.setDeliveryDelay(2000);
        assertEquals(2000, producer.getDeliveryDelay());
    }

    @Test
    @Timeout(10)
    public void testAnonymousProducerThrowsUOEWhenExplictDestinationNotProvided() throws Exception {
        JmsMessageProducer producer = (JmsMessageProducer) session.createProducer(null);

        Message message = Mockito.mock(Message.class);
        try {
            producer.send(message);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }

        try {
            producer.send(message, completionListener);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }

        try {
            producer.send(message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }

        try {
            producer.send(message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE, completionListener);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }
    }

    @Test
    @Timeout(10)
    public void testExplicitQueueProducerThrowsIDEWhenNullDestinationIsProvidedOnSend() throws Exception {
        doExplicitProducerThrowsIDEWhenNullDestinationIsProvidedOnSendTestImpl(new JmsQueue("explicitQueueDest"));
    }

    @Test
    @Timeout(10)
    public void testExplicitTopicProducerThrowsIDEWhenInvalidDestinationIsProvidedOnSend() throws Exception {
        doExplicitProducerThrowsIDEWhenNullDestinationIsProvidedOnSendTestImpl(new JmsTopic("explicitTopicDest"));
    }

    private void doExplicitProducerThrowsIDEWhenNullDestinationIsProvidedOnSendTestImpl(JmsDestination explicitDest) throws JMSException {
        JmsDestination invalildNullDest = null;
        JmsMessageProducer producer = (JmsMessageProducer) session.createProducer(explicitDest);

        Message message = Mockito.mock(Message.class);
        try {
            producer.send(invalildNullDest, message);
            fail("Expected exception to be thrown");
        } catch (InvalidDestinationException ide) {
            // expected
        }

        try {
            producer.send(invalildNullDest, message, completionListener);
            fail("Expected exception to be thrown");
        } catch (InvalidDestinationException ide) {
            // expected
        }

        try {
            producer.send(invalildNullDest, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            fail("Expected exception to be thrown");
        } catch (InvalidDestinationException ide) {
            // expected
        }

        try {
            producer.send(invalildNullDest, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE, completionListener);
            fail("Expected exception to be thrown");
        } catch (InvalidDestinationException ide) {
            // expected
        }
    }

    @Test
    @Timeout(10)
    public void testExplicitProducerThrowsUOEWhenExplictDestinationIsProvided() throws Exception {
        JmsDestination dest = new JmsQueue("explicitDestination");
        JmsMessageProducer producer = (JmsMessageProducer) session.createProducer(dest);

        Message message = Mockito.mock(Message.class);
        try {
            producer.send(dest, message);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }

        try {
            producer.send(dest, message, completionListener);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }

        try {
            producer.send(dest, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }

        try {
            producer.send(dest, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE, completionListener);
            fail("Expected exception not thrown");
        } catch (UnsupportedOperationException uoe) {
            // expected
        }
    }

    @Test
    @Timeout(10)
    public void testAnonymousDestinationProducerThrowsIDEWhenNullDestinationIsProvided() throws Exception {
        JmsMessageProducer producer = (JmsMessageProducer) session.createProducer(null);

        Message message = Mockito.mock(Message.class);
        try {
            producer.send(null, message);
            fail("Expected exception not thrown");
        } catch (InvalidDestinationException ide) {
            // expected
        }

        try {
            producer.send(null, message, completionListener);
            fail("Expected exception not thrown");
        } catch (InvalidDestinationException ide) {
            // expected
        }

        try {
            producer.send(null, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            fail("Expected exception not thrown");
        } catch (InvalidDestinationException ide) {
            // expected
        }

        try {
            producer.send(null, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE, completionListener);
            fail("Expected exception not thrown");
        } catch (InvalidDestinationException ide) {
            // expected
        }
    }

    @Test
    @Timeout(10)
    public void testAnonymousProducerThrowsIAEWhenNullCompletionListenerProvided() throws Exception {
        JmsMessageProducer producer = (JmsMessageProducer) session.createProducer(null);
        JmsDestination dest = new JmsQueue("explicitDestination");

        Message message = Mockito.mock(Message.class);

        try {
            producer.send(dest, message, null);
            fail("Expected exception not thrown");
        } catch (IllegalArgumentException iae) {
            // expected
        }

        try {
            producer.send(dest, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE, null);
            fail("Expected exception not thrown");
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    @Test
    @Timeout(10)
    public void testExplicitProducerThrowsIAEWhenNullCompletionListenerIsProvided() throws Exception {
        JmsDestination dest = new JmsQueue("explicitDestination");
        JmsMessageProducer producer = (JmsMessageProducer) session.createProducer(dest);

        Message message = Mockito.mock(Message.class);
        try {
            producer.send(message, null);
            fail("Expected exception not thrown");
        } catch (IllegalArgumentException iae) {
            // expected
        }

        try {
            producer.send(message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE, null);
            fail("Expected exception not thrown");
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    @Test
    @Timeout(10)
    public void testAnonymousProducerThrowsMFEWhenNullMessageProvided() throws Exception {
        JmsDestination dest = new JmsQueue("explicitDestination");
        JmsMessageProducer producer = (JmsMessageProducer) session.createProducer(null);

        try {
            producer.send(dest, (Message) null);
            fail("Expected exception not thrown");
        } catch (MessageFormatException mfe) {
            // expected
        }

        try {
            producer.send(dest, (Message) null, completionListener);
            fail("Expected exception not thrown");
        } catch (MessageFormatException mfe) {
            // expected
        }

        try {
            producer.send(dest, (Message) null, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            fail("Expected exception not thrown");
        } catch (MessageFormatException mfe) {
            // expected
        }

        try {
            producer.send(dest, (Message) null, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE, completionListener);
            fail("Expected exception not thrown");
        } catch (MessageFormatException mfe) {
            // expected
        }
    }

    @Test
    @Timeout(10)
    public void testExplicitProducerThrowsMFEWhenNullMessageProvided() throws Exception {
        JmsDestination dest = new JmsQueue("explicitDestination");
        JmsMessageProducer producer = (JmsMessageProducer) session.createProducer(dest);

        try {
            producer.send((Message) null);
            fail("Expected exception not thrown");
        } catch (MessageFormatException mfe) {
            // expected
        }

        try {
            producer.send((Message) null, completionListener);
            fail("Expected exception not thrown");
        } catch (MessageFormatException mfe) {
            // expected
        }

        try {
            producer.send((Message) null, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            fail("Expected exception not thrown");
        } catch (MessageFormatException mfe) {
            // expected
        }

        try {
            producer.send((Message) null, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE, completionListener);
            fail("Expected exception not thrown");
        } catch (MessageFormatException mfe) {
            // expected
        }
    }

    @Test
    @Timeout(10)
    public void testInOrderSendAcksCompletionsReturnInOrder() throws Exception {
        final int MESSAGE_COUNT = 3;

        JmsConnectionFactory factory = new JmsConnectionFactory(
            "mock://localhost?mock.delayCompletionCalls=true");

        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final Destination destination = new JmsQueue("explicitDestination");
        JmsMessageProducer producer = (JmsMessageProducer) session.createProducer(destination);
        final MyCompletionListener listener = new MyCompletionListener();

        sendMessages(MESSAGE_COUNT, producer, listener);

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return remotePeer.getPendingCompletions(destination).size() == MESSAGE_COUNT;
            }
        }), "Not all sends made it to the remote");

        remotePeer.completeAllPendingSends(destination);

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return listener.getCompletedSends().size() == MESSAGE_COUNT;
            }
        }), "Not all completions triggered");

        assertMessageCompletedInOrder(MESSAGE_COUNT, listener);

        connection.close();
    }

    @Test
    @Timeout(10)
    public void testReversedOrderSendAcksCompletionsReturnInOrder() throws Exception {
        final int MESSAGE_COUNT = 3;

        JmsConnectionFactory factory = new JmsConnectionFactory(
            "mock://localhost?mock.delayCompletionCalls=true");

        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final Destination destination = new JmsQueue("explicitDestination");
        JmsMessageProducer producer = (JmsMessageProducer) session.createProducer(destination);
        final MyCompletionListener listener = new MyCompletionListener();

        sendMessages(MESSAGE_COUNT, producer, listener);

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return remotePeer.getPendingCompletions(destination).size() == MESSAGE_COUNT;
            }
        }), "Not all sends made it to the remote");

        List<JmsOutboundMessageDispatch> pending = remotePeer.getPendingCompletions(destination);
        assertEquals(MESSAGE_COUNT, pending.size());
        Collections.reverse(pending);

        for (JmsOutboundMessageDispatch envelope : pending) {
            LOG.info("Trigger completion of message: {}", envelope.getMessage().getJMSMessageID());
            remotePeer.completePendingSend(envelope);
        }

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return listener.getCompletedSends().size() == MESSAGE_COUNT;
            }
        }), "Not all completions triggered");

        assertMessageCompletedInOrder(MESSAGE_COUNT, listener);

        connection.close();
    }

    @Test
    @Timeout(10)
    public void testInOrderSendFailuresCompletionsReturnInOrder() throws Exception {
        final int MESSAGE_COUNT = 3;

        JmsConnectionFactory factory = new JmsConnectionFactory(
            "mock://localhost?mock.delayCompletionCalls=true");

        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final Destination destination = new JmsQueue("explicitDestination");
        JmsMessageProducer producer = (JmsMessageProducer) session.createProducer(destination);
        final MyCompletionListener listener = new MyCompletionListener();

        sendMessages(MESSAGE_COUNT, producer, listener);
        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return remotePeer.getPendingCompletions(destination).size() == MESSAGE_COUNT;
            }
        }), "Not all messages sent");
        remotePeer.failAllPendingSends(destination, new ProviderException("Could not send message"));

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return listener.getFailedSends().size() == MESSAGE_COUNT;
            }
        }), "Not all completions triggered");

        assertMessageFailedInOrder(MESSAGE_COUNT, listener);

        connection.close();
    }

    @Test
    @Timeout(10)
    public void testReversedOrderSendAcksFailuresReturnInOrder() throws Exception {
        final int MESSAGE_COUNT = 3;

        JmsConnectionFactory factory = new JmsConnectionFactory(
            "mock://localhost?mock.delayCompletionCalls=true");

        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final Destination destination = new JmsQueue("explicitDestination");
        JmsMessageProducer producer = (JmsMessageProducer) session.createProducer(destination);
        final MyCompletionListener listener = new MyCompletionListener();

        sendMessages(MESSAGE_COUNT, producer, listener);

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return remotePeer.getPendingCompletions(destination).size() == MESSAGE_COUNT;
            }
        }), "Not all sends made it to the remote");

        List<JmsOutboundMessageDispatch> pending = remotePeer.getPendingCompletions(destination);
        assertEquals(MESSAGE_COUNT, pending.size());
        Collections.reverse(pending);

        for (JmsOutboundMessageDispatch envelope : pending) {
            LOG.info("Trigger failure of message: {}", envelope.getMessage().getJMSMessageID());
            remotePeer.failPendingSend(envelope, new ProviderException("Failed to send message"));
        }

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return listener.getFailedSends().size() == MESSAGE_COUNT;
            }
        }), "Not all failures triggered");

        assertMessageFailedInOrder(MESSAGE_COUNT, listener);

        connection.close();
    }

    @Test
    @Timeout(10)
    public void testInterleavedCompletionsReturnedInOrder() throws Exception {
        final int MESSAGE_COUNT = 3;

        JmsConnectionFactory factory = new JmsConnectionFactory(
            "mock://localhost?mock.delayCompletionCalls=true");

        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final Destination destination = new JmsQueue("explicitDestination");
        JmsMessageProducer producer = (JmsMessageProducer) session.createProducer(destination);
        final MyCompletionListener listener = new MyCompletionListener();

        sendMessages(MESSAGE_COUNT, producer, listener);

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return remotePeer.getPendingCompletions(destination).size() == MESSAGE_COUNT;
            }
        }), "Not all sends made it to the remote");

        List<JmsOutboundMessageDispatch> pending = remotePeer.getPendingCompletions(destination);
        assertEquals(MESSAGE_COUNT, pending.size());
        Collections.reverse(pending);

        for (JmsOutboundMessageDispatch envelope : pending) {
            int sequence = envelope.getMessage().getIntProperty("sequence");
            if (sequence % 2 == 0) {
                LOG.info("Trigger completion of message: {}", envelope.getMessage().getJMSMessageID());
                remotePeer.completePendingSend(envelope);
            } else {
                LOG.info("Trigger failure of message: {}", envelope.getMessage().getJMSMessageID());
                remotePeer.failPendingSend(envelope, new ProviderException("Failed to send message"));
            }
        }

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return listener.getCombinedSends().size() == MESSAGE_COUNT;
            }
        }), "Not all completions triggered");

        assertTotalCompletionOrder(MESSAGE_COUNT, listener);

        connection.close();
    }

    @Test
    @Timeout(15)
    public void testCompletionListenerOnCompleteCallsProducerCloseThrowsISE() throws Exception {
        final int MESSAGE_COUNT = 1;

        JmsConnectionFactory factory = new JmsConnectionFactory(
            "mock://localhost?mock.delayCompletionCalls=true");

        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final CountDownLatch done = new CountDownLatch(1);
        final Destination destination = new JmsQueue("explicitDestination");
        final JmsMessageProducer producer = (JmsMessageProducer) session.createProducer(destination);
        final CompletionListener listener = new CompletionListener() {

            @Override
            public void onCompletion(Message message) {
                try {
                    producer.close();
                } catch (IllegalStateException ex) {
                    done.countDown();
                } catch (Exception e) {
                    LOG.info("Wrong exception thrown when close called in completion: {}", e.getMessage());
                }
            }

            @Override
            public void onException(Message message, Exception exception) {
                LOG.info("Unexpected exception thrown in completion: {}", exception.getMessage());
            }
        };

        sendMessages(MESSAGE_COUNT, producer, listener);

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return remotePeer.getPendingCompletions(destination).size() == MESSAGE_COUNT;
            }
        }), "Not all sends made it to the remote");

        remotePeer.completeAllPendingSends(destination);

        assertTrue(done.await(10, TimeUnit.SECONDS), "Completion never got expected ISE");

        connection.close();
    }

    @Test
    @Timeout(15)
    public void testCompletionListenerOnExceptionCallsProducerCloseThrowsISE() throws Exception {
        final int MESSAGE_COUNT = 1;

        JmsConnectionFactory factory = new JmsConnectionFactory(
            "mock://localhost?mock.delayCompletionCalls=true");

        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final CountDownLatch done = new CountDownLatch(1);
        final Destination destination = new JmsQueue("explicitDestination");
        final JmsMessageProducer producer = (JmsMessageProducer) session.createProducer(destination);
        final CompletionListener listener = new CompletionListener() {

            @Override
            public void onCompletion(Message message) {
            }

            @Override
            public void onException(Message message, Exception exception) {
                try {
                    producer.close();
                } catch (IllegalStateException ex) {
                    done.countDown();
                } catch (Exception e) {
                    LOG.info("Wrong exception thrown when close called in completion: {}", e.getMessage());
                }
            }
        };

        sendMessages(MESSAGE_COUNT, producer, listener);

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return remotePeer.getPendingCompletions(destination).size() == MESSAGE_COUNT;
            }
        }), "Not all sends made it to the remote");

        remotePeer.failAllPendingSends(destination, new ProviderException("Could not send message"));

        assertTrue(done.await(10, TimeUnit.SECONDS), "Completion never got expected ISE");

        connection.close();
    }

    private void sendMessages(int count, JmsMessageProducer producer, CompletionListener listener) throws Exception {
        for (int i = 0; i < count; ++i) {
            Message message = session.createMessage();
            message.setIntProperty("sequence", i);

            producer.send(message, listener);
        }
    }

    private void assertMessageCompletedInOrder(int expected, MyCompletionListener listener) throws Exception {
        assertEquals(expected, listener.completed.size(), "Did not get expected number of completions");
        for (int i = 0; i < listener.completed.size(); ++i) {
            int sequence = listener.completed.get(i).getIntProperty("sequence");
            assertEquals(i, sequence, "Did not complete expected message: " + i + " got: " + sequence);
        }
    }

    private void assertMessageFailedInOrder(int expected, MyCompletionListener listener) throws Exception {
        assertEquals(expected, listener.failed.size(), "Did not get expected number of failures");
        for (int i = 0; i < listener.failed.size(); ++i) {
            int sequence = listener.failed.get(i).getIntProperty("sequence");
            assertEquals(i, sequence, "Did not fail expected message: " + i + " got: " + sequence);
        }
    }

    private void assertTotalCompletionOrder(int expected, MyCompletionListener listener) throws Exception {
        assertEquals(expected, listener.combinedResult.size(), "Did not get expected number of failures");
        for (int i = 0; i < listener.combinedResult.size(); ++i) {
            int sequence = listener.combinedResult.get(i).getIntProperty("sequence");
            assertEquals(i, sequence, "Did not fail expected message: " + i + " got: " + sequence);
        }
    }

    private class MyCompletionListener implements CompletionListener {

        private final List<Message> completed = new ArrayList<Message>();
        private final List<Message> failed = new ArrayList<Message>();
        private final List<Message> combinedResult = new ArrayList<Message>();

        @Override
        public void onCompletion(Message message) {
            try {
                LOG.debug("Recording completed send: {}", message.getJMSMessageID());
            } catch (JMSException e) {
            }
            completed.add(message);
            combinedResult.add(message);
        }

        @Override
        public void onException(Message message, Exception exception) {
            try {
                LOG.debug("Recording failed send: {} -> error {}", message.getJMSMessageID(), exception.getMessage());
            } catch (JMSException e) {
            }
            failed.add(message);
            combinedResult.add(message);
        }

        public List<Message> getCombinedSends() {
            return combinedResult;
        }

        public List<Message> getCompletedSends() {
            return completed;
        }

        public List<Message> getFailedSends() {
            return failed;
        }
    }
}
