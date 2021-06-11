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

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.NETWORK_HOST;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.OPEN_HOSTNAME;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.PORT;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.ResourceAllocationException;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionExtensions;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsConnectionRemotelyClosedException;
import org.apache.qpid.jms.JmsDefaultConnectionListener;
import org.apache.qpid.jms.provider.amqp.AmqpSupport;
import org.apache.qpid.jms.provider.exceptions.ProviderConnectionRedirectedException;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.Wait;
import org.apache.qpid.jms.test.testpeer.basictypes.AmqpError;
import org.apache.qpid.jms.test.testpeer.basictypes.ConnectionError;
import org.apache.qpid.jms.test.testpeer.matchers.CoordinatorMatcher;
import org.apache.qpid.jms.util.MetaDataSupport;
import org.apache.qpid.jms.util.QpidJMSTestRunner;
import org.apache.qpid.jms.util.Repeat;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.transaction.TxnCapability;
import org.apache.qpid.proton.engine.impl.AmqpHeader;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.apache.qpid.protonj2.test.driver.codec.security.SaslCode;
import org.apache.qpid.protonj2.test.driver.codec.transport.AMQPHeader;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(QpidJMSTestRunner.class)
public class ConnectionIntegrationTest extends QpidJmsTestCase {

    private final IntegrationTestFixture2 testFixture = new IntegrationTestFixture2();

    @Test(timeout = 20000)
    public void testCreateAndCloseConnection() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            testPeer.expectClose().respond();
            connection.close();
        }
    }

    @Test(timeout = 10000)
    public void testCreateConnectionToNonSaslPeer() throws Exception {
        doConnectionWithUnexpectedHeaderTestImpl(AmqpHeader.HEADER);
    }

    @Test(timeout = 10000)
    public void testCreateConnectionToNonAmqpPeer() throws Exception {
        byte[] responseHeader = new byte[] { 'N', 'O', 'T', '-', 'A', 'M', 'Q', 'P' };
        doConnectionWithUnexpectedHeaderTestImpl(responseHeader);
    }

    private void doConnectionWithUnexpectedHeaderTestImpl(byte[] responseHeader) throws Exception, IOException {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            testPeer.expectSASLHeader().respondWithBytes(responseHeader);
            testPeer.start();

            ConnectionFactory factory = new JmsConnectionFactory(testPeer.getServerURI());
            try {
                factory.createConnection("guest", "guest");
                fail("Expected connection creation to fail");
            } catch (JMSException jmse) {
                assertThat(jmse.getMessage(), containsString("SASL header mismatch"));
            }
        }
    }

    @Test(timeout = 20000)
    public void testCloseConnectionTimesOut() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            final JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setCloseTimeout(500);

            testPeer.expectClose();

            connection.start();
            assertNotNull("Connection should not be null", connection);
            connection.close();

            testPeer.waitForScriptToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testCloseConnectionCompletesWhenConnectionDropsBeforeResponse() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);

            testPeer.expectClose();
            testPeer.dropAfterLastHandler();

            connection.start();
            assertNotNull("Connection should not be null", connection);
            connection.close();

            testPeer.waitForScriptToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testCreateConnectionWithClientId() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            Connection connection = testFixture.establishConnecton(testPeer, false, null, null, null, true);
            testPeer.expectClose().respond();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testCreateAutoAckSession() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin().respond();
            testPeer.expectClose().respond();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            assertNotNull("Session should not be null", session);
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testCreateAutoAckSessionByDefault() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin().respond();
            testPeer.expectClose().respond();

            Session session = connection.createSession();
            assertNotNull("Session should not be null", session);
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testCreateAutoAckSessionUsingAckModeOnlyMethod() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin().respond();
            testPeer.expectClose().respond();

            Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            assertNotNull("Session should not be null", session);
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testCreateTransactedSession() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            // Expect the session, with an immediate link to the transaction coordinator
            // using a target with the expected capabilities only.
            testPeer.expectBegin().respond();
            testPeer.expectCoordinatorAttach().ofSender()
                                              .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and()
                                              .respond()
                                              .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString());
            testPeer.remoteFlow().withLinkCredit(2).queue();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            final byte[] txnId = new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4};

            testPeer.expectDeclare().declared(txnId);
            testPeer.expectDischarge().withTxnId(txnId).accept();
            testPeer.expectClose().respond();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            assertNotNull("Session should not be null", session);

            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testCreateTransactedSessionUsingAckModeOnlyMethod() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            // Expect the session, with an immediate link to the transaction coordinator
            // using a target with the expected capabilities only.
            testPeer.expectBegin().respond();
            testPeer.expectCoordinatorAttach().ofSender()
                                              .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString()).and()
                                              .respond()
                                              .withCoordinator().withCapabilities(TxnCapability.LOCAL_TXN.toString());
            testPeer.remoteFlow().withLinkCredit(2).queue();

            // First expect an unsettled 'declare' transfer to the txn coordinator, and
            // reply with a declared disposition state containing the txnId.
            final byte[] txnId = new byte[]{ (byte) 1, (byte) 2, (byte) 3, (byte) 4};

            testPeer.expectDeclare().declared(txnId);
            testPeer.expectDischarge().withTxnId(txnId).accept();
            testPeer.expectClose().respond();

            Session session = connection.createSession(Session.SESSION_TRANSACTED);
            assertNotNull("Session should not be null", session);

            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testCreateTransactedSessionFailsWhenNoDetachResponseSent() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            ((JmsConnection) connection).setRequestTimeout(500);

            testPeer.expectBegin().respond();
            // Expect the session, with an immediate link to the transaction coordinator
            // using a target with the expected capabilities only.
            CoordinatorMatcher txCoordinatorMatcher = new CoordinatorMatcher();
            txCoordinatorMatcher.withCapabilities(arrayContaining(TxnCapability.LOCAL_TXN));
            testPeer.expectCoordinatorAttach().ofSender().reject(true, AmqpError.INTERNAL_ERROR.toString(), "error");
            testPeer.expectDetach().withClosed(true);
            // Expect the AMQP session to be closed due to the JMS session creation failure.
            testPeer.expectEnd().respond();
            testPeer.expectClose().respond();

            try {
                connection.createSession(true, Session.SESSION_TRANSACTED);
                fail("Session create should have failed.");
            } catch (JMSException ex) {
                // Expected
            }

            connection.close();

            testPeer.waitForScriptToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyCloseConnectionDuringSessionCreation() throws Exception {
        final String BREAD_CRUMB = "ErrorMessageBreadCrumb";

        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            // Expect the begin, then explicitly close the connection with an error
            testPeer.expectBegin();
            testPeer.remoteClose().withErrorCondition(AmqpError.NOT_ALLOWED.toString(), BREAD_CRUMB).queue();
            testPeer.expectClose();

            try {
                connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                fail("Expected exception to be thrown");
            } catch (JMSException jmse) {
                // Expected
                assertNotNull("Expected exception to have a message", jmse.getMessage());
                assertTrue("Expected breadcrumb to be present in message", jmse.getMessage().contains(BREAD_CRUMB));
            }

            testPeer.waitForScriptToComplete(3000);

            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyDropConnectionDuringSessionCreation() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            // Expect the begin, then drop connection without without a close frame.
            testPeer.expectBegin();
            testPeer.dropAfterLastHandler();

            try {
                connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                fail("Expected exception to be thrown");
            } catch (JMSException jmse) {
                // Expected
            }

            testPeer.waitForScriptToComplete(3000);

            connection.close();
        }
    }

    @Repeat(repetitions = 1)
    @Test(timeout = 20000)
    public void testRemotelyDropConnectionDuringSessionCreationTransacted() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            testPeer.expectSASLAnonymousConnect();
            testPeer.expectOpen().respond();
            testPeer.expectBegin().respond();
            testPeer.start();

            ConnectionFactory factory = new JmsConnectionFactory(testPeer.getServerURI().toString() + "?jms.clientID=foo");
            Connection connection = factory.createConnection();

            CountDownLatch exceptionListenerFired = new CountDownLatch(1);
            connection.setExceptionListener(ex -> exceptionListenerFired.countDown());

            // Expect the begin, then drop connection without without a close frame before the tx-coordinator setup.
            testPeer.expectBegin();
            testPeer.dropAfterLastHandler();

            try {
                connection.createSession(true, Session.SESSION_TRANSACTED);
                fail("Expected exception to be thrown");
            } catch (JMSException jmse) {
                // Expected
            }

            assertTrue("Exception listener did not fire", exceptionListenerFired.await(5, TimeUnit.SECONDS));

            testPeer.waitForScriptToComplete(3000);

            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testConnectionPropertiesContainExpectedMetaData() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {

            Map<String, Object> expectedProperties = new HashMap<>();
            expectedProperties.put(AmqpSupport.PRODUCT.toString(), MetaDataSupport.PROVIDER_NAME);
            expectedProperties.put(AmqpSupport.VERSION.toString(), MetaDataSupport.PROVIDER_VERSION);
            expectedProperties.put(AmqpSupport.PLATFORM.toString(), MetaDataSupport.PLATFORM_DETAILS);

            testPeer.expectSASLAnonymousConnect();
            testPeer.expectOpen().withProperties(expectedProperties).respond();
            testPeer.expectBegin().respond();
            testPeer.start();

            ConnectionFactory factory = new JmsConnectionFactory(testPeer.getServerURI() + "?jms.clientID=foo");
            Connection connection = factory.createConnection();

            testPeer.waitForScriptToComplete(1000);

            testPeer.expectClose().respond();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testMaxFrameSizeOptionCommunicatedInOpen() throws Exception {
        final int frameSize = 39215;

        doMaxFrameSizeOptionTestImpl(frameSize, frameSize);
    }

    @Test(timeout = 20000)
    public void testMaxFrameSizeOptionCommunicatedInOpenDefault() throws Exception {
        doMaxFrameSizeOptionTestImpl(-1, UnsignedInteger.MAX_VALUE.longValue());
    }

    private void doMaxFrameSizeOptionTestImpl(int uriOption, long transmittedValue) throws JMSException, InterruptedException, Exception, IOException {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            testPeer.expectAMQPHeader().respondWithAMQPHeader();
            testPeer.expectOpen().withMaxFrameSize(transmittedValue).respond();
            testPeer.expectBegin().respond();
            testPeer.start();

            String uri = testPeer.getServerURI().toString() + "?amqp.saslLayer=false&amqp.maxFrameSize=" + uriOption;
            ConnectionFactory factory = new JmsConnectionFactory(uri);
            Connection connection = factory.createConnection();
            connection.start();

            testPeer.waitForScriptToComplete(3000);

            testPeer.expectClose().respond();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testMaxFrameSizeInfluencesOutgoingFrameSize() throws Exception {
        doMaxFrameSizeInfluencesOutgoingFrameSizeTestImpl(1000, 10001, 11);
        doMaxFrameSizeInfluencesOutgoingFrameSizeTestImpl(1500, 6001, 5);
    }

    private void doMaxFrameSizeInfluencesOutgoingFrameSizeTestImpl(int frameSize, int bytesPayloadSize, int numFrames) throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            testPeer.expectAMQPHeader().respondWithAMQPHeader();
            testPeer.expectOpen().withMaxFrameSize(frameSize).respond();
            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin().respond();
            testPeer.expectBegin().respond();
            testPeer.expectAttach().ofSender().respond();
            testPeer.remoteFlow().withLinkCredit(1).queue();
            testPeer.start();

            String uri = testPeer.getServerURI().toString() + "?amqp.saslLayer=false&amqp.maxFrameSize=" + frameSize;

            ConnectionFactory factory = new JmsConnectionFactory(uri);
            Connection connection = factory.createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            // Expect n-1 transfers of maxFrameSize
            for (int i = 1; i < numFrames; i++) {
                testPeer.expectTransfer().withMore(true).withFrameSize(frameSize);
            }
            // Plus one more of unknown size (framing overhead).
            testPeer.expectTransfer().withMore(Matchers.oneOf(null, false)).accept();

            // Send the message
            byte[] orig = createBytePyload(bytesPayloadSize);
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(orig);

            producer.send(message);

            testPeer.expectClose().respond();
            connection.close();

            testPeer.waitForScriptToComplete(3000);
        }
    }

    private static byte[] createBytePyload(int sizeInBytes) {
        Random rand = new Random(System.currentTimeMillis());

        byte[] payload = new byte[sizeInBytes];
        for (int i = 0; i < sizeInBytes; i++) {
            payload[i] = (byte) (64 + 1 + rand.nextInt(9));
        }

        return payload;
    }

    @Test(timeout = 20000)
    public void testAmqpHostnameSetByDefault() throws Exception {
        doAmqpHostnameTestImpl("localhost", false, equalTo("localhost"));
    }

    @Test(timeout = 20000)
    public void testAmqpHostnameSetByVhostOption() throws Exception {
        String vhost = "myAmqpHost";
        doAmqpHostnameTestImpl(vhost, true, equalTo(vhost));
    }

    @Test(timeout = 20000)
    public void testAmqpHostnameNotSetWithEmptyVhostOption() throws Exception {
        doAmqpHostnameTestImpl("", true, nullValue());
    }

    private void doAmqpHostnameTestImpl(String amqpHostname, boolean setHostnameOption, Matcher<?> hostnameMatcher) throws JMSException, InterruptedException, Exception, IOException {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            testPeer.expectSASLAnonymousConnect();
            testPeer.expectOpen().withHostname(hostnameMatcher).respond();
            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin().respond();
            testPeer.start();

            String uri = testPeer.getServerURI().toString();
            if (setHostnameOption) {
                uri += "?amqp.vhost=" + amqpHostname;
            }

            ConnectionFactory factory = new JmsConnectionFactory(uri);
            Connection connection = factory.createConnection();
            // Set a clientID to provoke the actual AMQP connection process to occur.
            connection.setClientID("clientName");

            testPeer.waitForScriptToComplete(1000);
            testPeer.expectClose().respond();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyEndConnectionListenerInvoked() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            final CountDownLatch done = new CountDownLatch(1);

            // Don't set a ClientId, so that the underlying AMQP connection isn't established yet
            Connection connection = testFixture.establishConnecton(testPeer, false, null, null, null, false);

            // Tell the test peer to close the connection when executing its last handler
            testPeer.remoteClose().queue();
            testPeer.expectClose();

            // Add the exception listener
            connection.setExceptionListener(new ExceptionListener() {

                @Override
                public void onException(JMSException exception) {
                    done.countDown();
                }
            });

            // Trigger the underlying AMQP connection
            connection.start();

            assertTrue("Connection should report failure", done.await(5, TimeUnit.SECONDS));

            testPeer.waitForScriptToComplete(1000);

            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyEndConnectionWithRedirect() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            final CountDownLatch done = new CountDownLatch(1);
            final AtomicReference<JMSException> asyncError = new AtomicReference<JMSException>();

            final String redirectVhost = "vhost";
            final String redirectNetworkHost = "localhost";
            final int redirectPort = 5677;

            // Don't set a ClientId, so that the underlying AMQP connection isn't established yet
            Connection connection = testFixture.establishConnecton(testPeer, false, null, null, null, false);

            // Tell the test peer to close the connection when executing its last handler
            Map<String, Object> errorInfo = new HashMap<>();
            errorInfo.put(OPEN_HOSTNAME.toString(), redirectVhost);
            errorInfo.put(NETWORK_HOST.toString(), redirectNetworkHost);
            errorInfo.put(PORT.toString(), 5677);

            testPeer.remoteClose().withErrorCondition(ConnectionError.REDIRECT.toString(), "Connection redirected", errorInfo).queue();
            testPeer.expectClose();

            // Add the exception listener
            connection.setExceptionListener(new ExceptionListener() {

                @Override
                public void onException(JMSException exception) {
                    asyncError.set(exception);
                    done.countDown();
                }
            });

            // Trigger the underlying AMQP connection
            connection.start();

            assertTrue("Connection should report failure", done.await(5, TimeUnit.SECONDS));

            assertTrue(asyncError.get() instanceof JMSException);
            assertTrue(asyncError.get().getCause() instanceof ProviderConnectionRedirectedException);

            ProviderConnectionRedirectedException redirect = (ProviderConnectionRedirectedException) asyncError.get().getCause();
            URI redirectionURI = redirect.getRedirectionURI();

            assertNotNull(redirectionURI);
            assertTrue(redirectVhost, redirectionURI.getQuery().contains("amqp.vhost=" + redirectVhost));
            assertEquals(redirectNetworkHost, redirectionURI.getHost());
            assertEquals(redirectPort, redirectionURI.getPort());

            testPeer.waitForScriptToComplete(1000);

            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testRemotelyEndConnectionWithSessionWithConsumer() throws Exception {
        final String BREAD_CRUMB = "ErrorMessage";

        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            final Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin().respond();

            // Create a consumer, then remotely end the connection afterwards.
            testPeer.expectAttach().ofReceiver().respond();
            testPeer.expectFlow();
            testPeer.remoteClose().withErrorCondition(AmqpError.RESOURCE_LIMIT_EXCEEDED.toString(), BREAD_CRUMB).queue();
            testPeer.expectClose();
            testPeer.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageConsumer consumer = session.createConsumer(queue);

            testPeer.waitForScriptToComplete(1000);
            assertTrue("connection never closed.", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisfied() throws Exception {
                    return !((JmsConnection) connection).isConnected();
                }
            }, 10000, 10));

            try {
                connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                fail("Expected ISE to be thrown due to being closed");
            } catch (IllegalStateException jmsise) {
                String message = jmsise.getCause().getMessage();
                assertTrue(message.contains(AmqpError.RESOURCE_LIMIT_EXCEEDED.toString()));
                assertTrue(message.contains(BREAD_CRUMB));
            }

            // Verify the session is now marked closed
            try {
                session.getAcknowledgeMode();
                fail("Expected ISE to be thrown due to being closed");
            } catch (IllegalStateException jmsise) {
                String message = jmsise.getCause().getMessage();
                assertTrue(message.contains(AmqpError.RESOURCE_LIMIT_EXCEEDED.toString()));
                assertTrue(message.contains(BREAD_CRUMB));
            }

            // Verify the consumer is now marked closed
            try {
                consumer.getMessageListener();
                fail("Expected ISE to be thrown due to being closed");
            } catch (IllegalStateException jmsise) {
                String message = jmsise.getCause().getMessage();
                assertTrue(message.contains(AmqpError.RESOURCE_LIMIT_EXCEEDED.toString()));
                assertTrue(message.contains(BREAD_CRUMB));
            }

            // Try closing them explicitly, should effectively no-op in client.
            // The test peer will throw during close if it sends anything.
            consumer.close();
            session.close();
            connection.close();

            testPeer.waitForScriptToComplete(10);
        }
    }

    @Test(timeout = 20000)
    public void  testRemotelyEndConnectionWithSessionWithProducerWithSendWaitingOnCredit() throws Exception {
        final String BREAD_CRUMB = "ErrorMessageBreadCrumb";

        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin().respond();
            // Expect producer creation, don't give it credit.
            testPeer.expectAttach().ofSender().respond();
            // Producer has no credit so the send should block waiting for it.
            testPeer.remoteClose().withErrorCondition(AmqpError.RESOURCE_LIMIT_EXCEEDED.toString(), BREAD_CRUMB).queue().afterDelay(50);
            testPeer.expectClose();

            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queue = session.createQueue("myQueue");
            final MessageProducer producer = session.createProducer(queue);
            final Message message = session.createTextMessage("myMessage");

            try {
                producer.send(message);
                fail("Expected exception to be thrown");
            } catch (ResourceAllocationException jmse) {
                // Expected
                assertNotNull("Expected exception to have a message", jmse.getMessage());
                assertTrue("Expected breadcrumb to be present in message", jmse.getMessage().contains(BREAD_CRUMB));
            } catch (Throwable t) {
                fail("Caught unexpected exception: " + t);
            }

            connection.close();

            testPeer.waitForScriptToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void  testRemotelyEndConnectionWithSessionWithProducerWithSendWaitingOnOutcome() throws Exception {
        final String BREAD_CRUMB = "ErrorMessageBreadCrumb";

        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            final Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin().respond();
            // Expect producer creation, and a message to be sent, but don't return a disposition.
            // Instead, close the connection.
            testPeer.expectAttach().ofSender().respond();
            testPeer.remoteFlow().withLinkCredit(10).queue();
            testPeer.expectTransfer();
            testPeer.remoteClose().withErrorCondition(ConnectionError.CONNECTION_FORCED.toString(), BREAD_CRUMB).queue().afterDelay(50);
            testPeer.expectClose();

            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queue = session.createQueue("myQueue");
            final MessageProducer producer = session.createProducer(queue);

            Message message = session.createTextMessage("myMessage");

            try {
                producer.send(message);
                fail("Expected exception to be thrown");
            } catch (JmsConnectionRemotelyClosedException jmse) {
                // Expected
                assertNotNull("Expected exception to have a message", jmse.getMessage());
                assertTrue("Expected breadcrumb to be present in message", jmse.getMessage().contains(BREAD_CRUMB));
            } catch (Throwable t) {
                fail("Caught unexpected exception: " + t);
            }

            connection.close();

            testPeer.waitForScriptToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testCreateConnectionWithServerSendingPreemptiveData() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            testPeer.remoteHeader(AMQPHeader.getSASLHeader()).queue();
            testPeer.remoteSaslMechanisms().withMechanisms("ANONYMOUS").queue();
            testPeer.expectSASLHeader();
            testPeer.expectSaslInit().withMechanism("ANONYMOUS");
            testPeer.remoteSaslOutcome().withCode(SaslCode.OK).queue();
            testPeer.expectAMQPHeader().respondWithAMQPHeader();
            testPeer.expectOpen().respond();
            testPeer.expectBegin().respond();
            testPeer.start();

            JmsConnectionFactory factory = new JmsConnectionFactory(testPeer.getServerURI());
            Connection connection = factory.createConnection();
            connection.start();

            testPeer.expectClose().respond();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testDontAwaitClientIDBeforeOpen() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            testPeer.expectSASLAnonymousConnect();
            testPeer.expectOpen().respond();
            testPeer.expectBegin().respond();
            testPeer.start();

            String uri = testPeer.getServerURI().toString() + "?jms.awaitClientID=false";

            ConnectionFactory factory = new JmsConnectionFactory(uri);
            Connection connection = factory.createConnection();

            // Verify that all handlers complete, i.e. the awaitClientID=false option
            // setting was effective in provoking the AMQP Open immediately even
            // though it has no ClientID and we haven't used the Connection.
            testPeer.waitForScriptToComplete(3000);

            testPeer.expectClose().respond();
            connection.close();

            testPeer.waitForScriptToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testWaitForClientIDDoesNotOpenUntilPromptedWithSetClientID() throws Exception {
        doTestWaitForClientIDDoesNotOpenUntilPrompted(true);
    }

    @Test(timeout = 20000)
    public void testWaitForClientIDDoesNotOpenUntilPromptedWithStart() throws Exception {
        doTestWaitForClientIDDoesNotOpenUntilPrompted(false);
    }

    private void doTestWaitForClientIDDoesNotOpenUntilPrompted(boolean setClientID) throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            testPeer.expectSASLAnonymousConnect();
            testPeer.start();

            String uri = testPeer.getServerURI().toString() + "?jms.awaitClientID=true";
            ConnectionFactory factory = new JmsConnectionFactory(uri);
            Connection connection = factory.createConnection();

            testPeer.waitForScriptToComplete(1000);

            testPeer.expectOpen().respond();
            testPeer.expectBegin().respond();

            if (setClientID) {
                connection.setClientID("client-id");
            } else {
                connection.start();
            }

            testPeer.expectClose().respond();
            connection.close();

            testPeer.waitForScriptToComplete(2000);
        }
    }

    @Test(timeout = 20000)
    public void testUseDaemonThreadURIOption() throws Exception {
        doUseDaemonThreadTestImpl(null);
        doUseDaemonThreadTestImpl(false);
        doUseDaemonThreadTestImpl(true);
    }

    private void doUseDaemonThreadTestImpl(Boolean useDaemonThread) throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            testPeer.expectSASLAnonymousConnect();
            testPeer.expectOpen().respond();
            testPeer.expectBegin().respond();
            testPeer.expectClose().respond();
            testPeer.start();

            String remoteURI = testPeer.getServerURI().toString();
            if (useDaemonThread != null) {
                remoteURI += "?jms.useDaemonThread=" + useDaemonThread;
            }

            final CountDownLatch connectionEstablished = new CountDownLatch(1);
            final AtomicBoolean daemonThread = new AtomicBoolean(false);

            ConnectionFactory factory = new JmsConnectionFactory(remoteURI);
            JmsConnection connection = (JmsConnection) factory.createConnection();

            connection.addConnectionListener(new JmsDefaultConnectionListener() {
                @Override
                public void onConnectionEstablished(URI remoteURI) {
                    // Record whether the thread is daemon or not
                    daemonThread.set(Thread.currentThread().isDaemon());

                    connectionEstablished.countDown();
                }
            });

            connection.start();

            assertTrue("Connection established callback didn't trigger", connectionEstablished.await(5, TimeUnit.SECONDS));

            connection.close();

            testPeer.waitForScriptToComplete(2000);

            if (useDaemonThread == null) {
                // Expect default to be false when not configured
                assertFalse(daemonThread.get());
            } else {
                // Else expect to match the URI option value
                assertEquals(useDaemonThread, daemonThread.get());
            }
        }
    }

    @Test(timeout = 20000)
    public void testConnectionWithPreemptiveServerOpen() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            testPeer.expectSASLHeader().respondWithSASLPHeader();
            testPeer.remoteSaslMechanisms().withMechanisms("ANONYMOUS").queue();
            testPeer.expectSaslInit().withMechanism("ANONYMOUS");
            testPeer.remoteSaslOutcome().withCode(SaslCode.OK).queue();
            testPeer.remoteHeader(AMQPHeader.getAMQPHeader()).queue();
            testPeer.remoteOpen().queue();
            // Then expect the clients header to arrive, but defer responding since the servers was already sent.
            testPeer.expectAMQPHeader();
            // Then expect the clients Open frame to arrive, but defer responding since the servers was already sent
            // before the clients AMQP connection open is provoked.
            testPeer.expectOpen().withContainerId("client-id");
            testPeer.expectBegin().respond();
            testPeer.start();

            // Ensure the Connection awaits a ClientID being set or not, giving time for the preemptive server Open
            final String uri = testPeer.getServerURI().toString() + "?jms.awaitClientID=true";

            ConnectionFactory factory = new JmsConnectionFactory(uri);
            Connection connection = factory.createConnection();

            Thread.sleep(10); // Gives a little more time for the preemptive Open to actually arrive.

            // Use the connection to provoke the Open
            connection.setClientID("client-id");

            testPeer.expectClose().respond();
            connection.close();

            testPeer.waitForScriptToComplete(2000);
        }
    }

    @Test(timeout = 20000)
    public void testConnectionPropertiesExtensionAddedValues() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            final String property1 = "property1";
            final String property2 = "property2";

            final String value1 = UUID.randomUUID().toString();
            final String value2 = UUID.randomUUID().toString();

            final Map<String, Object> expectedProperties = new HashMap<>();
            expectedProperties.put(property1, value1);
            expectedProperties.put(property2, value2);
            expectedProperties.put(AmqpSupport.PRODUCT.toString(), MetaDataSupport.PROVIDER_NAME);
            expectedProperties.put(AmqpSupport.VERSION.toString(), MetaDataSupport.PROVIDER_VERSION);
            expectedProperties.put(AmqpSupport.PLATFORM.toString(), MetaDataSupport.PLATFORM_DETAILS);

            testPeer.expectSASLAnonymousConnect();
            testPeer.expectOpen().withProperties(expectedProperties).respond();
            testPeer.expectBegin().respond();
            testPeer.start();

            final URI remoteURI = testPeer.getServerURI();

            JmsConnectionFactory factory = new JmsConnectionFactory(remoteURI);

            factory.setExtension(JmsConnectionExtensions.AMQP_OPEN_PROPERTIES.toString(), (connection, uri) -> {
                Map<String, Object> properties = new HashMap<>();

                properties.put(property1, value1);
                properties.put(property2, value2);

                return properties;
            });

            Connection connection = factory.createConnection();
            connection.start();

            testPeer.waitForScriptToComplete(1000);
            testPeer.expectClose().respond();

            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testConnectionPropertiesExtensionAddedValuesOfNonString() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            final String property1 = "property1";
            final String property2 = "property2";

            final UUID value1 = UUID.randomUUID();
            final UUID value2 = UUID.randomUUID();

            final Map<String, Object> expectedProperties = new HashMap<>();

            expectedProperties.put(property1, value1);
            expectedProperties.put(property2, value2);
            expectedProperties.put(AmqpSupport.PRODUCT.toString(), MetaDataSupport.PROVIDER_NAME);
            expectedProperties.put(AmqpSupport.VERSION.toString(), MetaDataSupport.PROVIDER_VERSION);
            expectedProperties.put(AmqpSupport.PLATFORM.toString(), MetaDataSupport.PLATFORM_DETAILS);

            testPeer.expectSASLAnonymousConnect();
            testPeer.expectOpen().withProperties(expectedProperties).respond();
            testPeer.expectBegin().respond();
            testPeer.start();

            final URI remoteURI = testPeer.getServerURI();

            JmsConnectionFactory factory = new JmsConnectionFactory(remoteURI);

            factory.setExtension(JmsConnectionExtensions.AMQP_OPEN_PROPERTIES.toString(), (connection, uri) -> {
                Map<String, Object> properties = new HashMap<>();

                properties.put(property1, value1);
                properties.put(property2, value2);

                return properties;
            });

            Connection connection = factory.createConnection();
            connection.start();

            testPeer.waitForScriptToComplete(1000);
            testPeer.expectClose().respond();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testConnectionPropertiesExtensionProtectsClientProperties() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {

            final Map<String, Object> expectedProperties = new HashMap<>();
            expectedProperties.put(AmqpSupport.PRODUCT.toString(), MetaDataSupport.PROVIDER_NAME);
            expectedProperties.put(AmqpSupport.VERSION.toString(), MetaDataSupport.PROVIDER_VERSION);
            expectedProperties.put(AmqpSupport.PLATFORM.toString(), MetaDataSupport.PLATFORM_DETAILS);

            testPeer.expectSASLAnonymousConnect();
            testPeer.expectOpen().withProperties(expectedProperties).respond();
            testPeer.expectBegin().respond();
            testPeer.start();

            final URI remoteURI = testPeer.getServerURI();

            JmsConnectionFactory factory = new JmsConnectionFactory(remoteURI);

            factory.setExtension(JmsConnectionExtensions.AMQP_OPEN_PROPERTIES.toString(), (connection, uri) -> {
                Map<String, Object> properties = new HashMap<>();

                properties.put(AmqpSupport.PRODUCT.toString(), "Super-Duper-Qpid-JMS");
                properties.put(AmqpSupport.VERSION.toString(), "5.0.32.Final");
                properties.put(AmqpSupport.PLATFORM.toString(), "Commodore 64");

                return properties;
            });

            Connection connection = factory.createConnection();
            connection.start();

            testPeer.waitForScriptToComplete(1000);
            testPeer.expectClose().respond();
            connection.close();
        }
    }

    @Test(timeout = 20000)
    public void testConnectionFailsWhenUserSuppliesIllegalProperties() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            testPeer.expectSASLAnonymousConnect();
            testPeer.start();

            final URI remoteURI = testPeer.getServerURI();

            JmsConnectionFactory factory = new JmsConnectionFactory(remoteURI);

            factory.setExtension(JmsConnectionExtensions.AMQP_OPEN_PROPERTIES.toString(), (connection, uri) -> {
                Map<String, Object> properties = new HashMap<>();

                properties.put("not-amqp-encodable", factory);

                return properties;
            });

            Connection connection = factory.createConnection();

            try {
                connection.start();
                fail("Should not be able to connect when illegal types are in the properties");
            } catch (JMSException ex) {
            } catch (Exception unexpected) {
                fail("Caught unexpected error from connnection.start() : " + unexpected);
            }

            testPeer.waitForScriptToComplete(1000);
        }
    }

    @Ignore("Disabled due to requirement of hard coded port")
    @Test(timeout = 20000)
    public void testLocalPortOption() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            testPeer.expectSASLAnonymousConnect();
            testPeer.expectOpen().respond();
            // Each connection creates a session for managing temporary destinations etc
            testPeer.expectBegin().respond();
            testPeer.start();

            int localPort = 5671;
            String uri = testPeer.getServerURI().toString() + "?transport.localPort=" + localPort;
            ConnectionFactory factory = new JmsConnectionFactory(uri);
            Connection connection = factory.createConnection();
            connection.start();

            testPeer.waitForScriptToComplete(2000);

            testPeer.expectClose().respond();
            connection.close();

            final int clientPort = testPeer.getConnectionRemotePort();

            assertEquals(localPort, clientPort);
        }
    }
}
