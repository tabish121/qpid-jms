/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.qpid.jms.integration;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.CompletionListener;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.MessageAnnotationsDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.PropertiesDescribedType;
import org.apache.qpid.protonj2.test.driver.ProtonTestServer;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.HeaderMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.MessageAnnotationsMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.PropertiesMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.transport.TransferPayloadCompositeMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedDataMatcher;
import org.junit.Test;

public class BytesMessageIntegrationTest extends QpidJmsTestCase {

    private final IntegrationTestFixture2 testFixture = new IntegrationTestFixture2();

    @Test(timeout = 20000)
    public void testSendBasicBytesMessageWithContent() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            Connection connection = testFixture.establishConnecton(testPeer);

            final byte[] content = "myBytes".getBytes();

            testPeer.expectBegin().respond();
            testPeer.expectAttach().ofSender().respond();
            testPeer.remoteFlow().withLinkCredit(1).queue();

            HeaderMatcher headersMatcher = new HeaderMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsMatcher msgAnnotationsMatcher = new MessageAnnotationsMatcher(true);
            msgAnnotationsMatcher.withEntry(AmqpMessageSupport.JMS_MSG_TYPE.toString(), equalTo(AmqpMessageSupport.JMS_BYTES_MESSAGE));
            PropertiesMatcher propertiesMatcher = new PropertiesMatcher(true);
            propertiesMatcher.withContentType(AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE.toString());
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedDataMatcher(content));

            testPeer.expectTransfer().withPayload(messageMatcher).accept();
            testPeer.expectClose().respond();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(content);

            producer.send(message);

            connection.close();

            testPeer.waitForScriptToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testReceiveBytesMessageUsingDataSectionWithContentTypeOctectStream() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            final byte[] expectedContent = "expectedContent".getBytes();

            testPeer.expectBegin().respond();
            testPeer.expectAttach().ofReceiver().respond();
            testPeer.expectFlow().withLinkCredit(notNullValue());
            testPeer.remoteTransfer().withProperties().withContentType(AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE.toString())
                                     .also()
                                     .withMessageAnnotations().withAnnotation(AmqpMessageSupport.JMS_MSG_TYPE.toString(), AmqpMessageSupport.JMS_BYTES_MESSAGE)
                                     .also()
                                     .withBody().withData(expectedContent)
                                     .also()
                                     .withDeliveryId(0).queue();
            testPeer.expectDisposition().withState().accepted().withSettled(true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);

            testPeer.waitForScriptToComplete(3000);

            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof BytesMessage);
            BytesMessage bytesMessage = (BytesMessage) receivedMessage;
            assertEquals(expectedContent.length, bytesMessage.getBodyLength());
            byte[] recievedContent = new byte[expectedContent.length];
            int readBytes = bytesMessage.readBytes(recievedContent);
            assertEquals(recievedContent.length, readBytes);
            assertTrue(Arrays.equals(expectedContent, recievedContent));

            testPeer.expectClose().respond();
            connection.close();

            testPeer.waitForScriptToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testReceiveBytesMessageUsingDataSectionWithContentTypeOctectStreamNoTypeAnnotation() throws Exception {
        doReceiveBasicBytesMessageUsingDataSectionTestImpl(AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE.toString());
    }

    @Test(timeout = 20000)
    public void testReceiveBasicBytesMessageUsingDataSectionWithContentTypeEmptyNoTypeAnnotation() throws Exception {
        doReceiveBasicBytesMessageUsingDataSectionTestImpl("");
    }

    @Test(timeout = 20000)
    public void testReceiveBasicBytesMessageUsingDataSectionWithContentTypeUnknownNoTypeAnnotation() throws Exception {
        doReceiveBasicBytesMessageUsingDataSectionTestImpl("type/unknown");
    }

    @Test(timeout = 20000)
    public void testReceiveBasicBytesMessageUsingDataSectionWithContentTypeNotSetNoTypeAnnotation() throws Exception {
        doReceiveBasicBytesMessageUsingDataSectionTestImpl(null);
    }

    private void doReceiveBasicBytesMessageUsingDataSectionTestImpl(String contentType) throws JMSException, InterruptedException, Exception, IOException {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            final byte[] expectedContent = "expectedContent".getBytes();

            testPeer.expectBegin().respond();
            testPeer.expectAttach().ofReceiver().respond();
            testPeer.expectFlow().withLinkCredit(notNullValue());
            testPeer.remoteTransfer().withProperties().withContentType(contentType == null ? null : contentType)
                                     .also()
                                     .withBody().withData(expectedContent)
                                     .also()
                                     .withDeliveryId(0).queue();
            testPeer.expectDisposition().withState().accepted().withSettled(true);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);

            testPeer.waitForScriptToComplete(3000);

            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof BytesMessage);
            BytesMessage bytesMessage = (BytesMessage) receivedMessage;
            assertEquals(expectedContent.length, bytesMessage.getBodyLength());
            byte[] recievedContent = new byte[expectedContent.length];
            int readBytes = bytesMessage.readBytes(recievedContent);
            assertEquals(recievedContent.length, readBytes);
            assertTrue(Arrays.equals(expectedContent, recievedContent));

            testPeer.expectClose().respond();
            connection.close();

            testPeer.waitForScriptToComplete(3000);
        }
    }

    /**
     * Test that a message received from the test peer with a Data section and content type of
     * {@link AmqpMessageSupport#OCTET_STREAM_CONTENT_TYPE} is returned as a BytesMessage, verify it
     * gives the expected data values when read, and when reset and left mid-stream before being
     * resent that it results in the expected AMQP data body section and properties content type
     * being received by the test peer.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceiveBytesMessageAndResendAfterResetAndPartialRead() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin().respond();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Prepare an AMQP message for the test peer to send, containing the content type and
            // a data body section populated with expected bytes for use as a JMS BytesMessage
            PropertiesDescribedType properties = new PropertiesDescribedType();
            properties.setContentType(AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE.toString());

            MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(AmqpMessageSupport.JMS_MSG_TYPE.toString(), AmqpMessageSupport.JMS_BYTES_MESSAGE);

            boolean myBool = true;
            byte myByte = 4;
            byte[] myBytes = "myBytes".getBytes();
            char myChar = 'd';
            double myDouble = 1234567890123456789.1234;
            float myFloat = 1.1F;
            int myInt = Integer.MAX_VALUE;
            long myLong = Long.MAX_VALUE;
            short myShort = 25;
            String myUTF = "myString";

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            dos.writeBoolean(myBool);
            dos.writeByte(myByte);
            dos.write(myBytes);
            dos.writeChar(myChar);
            dos.writeDouble(myDouble);
            dos.writeFloat(myFloat);
            dos.writeInt(myInt);
            dos.writeLong(myLong);
            dos.writeShort(myShort);
            dos.writeUTF(myUTF);

            byte[] bytesPayload = baos.toByteArray();

            // receive the message from the test peer
            testPeer.expectAttach().ofReceiver().respond();
            testPeer.expectFlow().withLinkCredit(notNullValue());
            testPeer.remoteTransfer().withProperties().withContentType(AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE.toString())
                                     .also()
                                     .withMessageAnnotations().withAnnotation(AmqpMessageSupport.JMS_MSG_TYPE.toString(), AmqpMessageSupport.JMS_BYTES_MESSAGE)
                                     .also()
                                     .withBody().withData(bytesPayload)
                                     .also()
                                     .withDeliveryId(0).queue();
            testPeer.expectDisposition().withState().accepted().withSettled(true);

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);
            testPeer.waitForScriptToComplete(3000);

            // verify the content is as expected
            assertNotNull("Message was not received", receivedMessage);
            assertTrue("Message was not a BytesMessage", receivedMessage instanceof BytesMessage);
            BytesMessage receivedBytesMessage = (BytesMessage) receivedMessage;

            assertEquals("Unexpected boolean value", myBool, receivedBytesMessage.readBoolean());
            assertEquals("Unexpected byte value", myByte, receivedBytesMessage.readByte());
            byte[] readBytes = new byte[myBytes.length];
            assertEquals("Did not read the expected number of bytes", myBytes.length, receivedBytesMessage.readBytes(readBytes));
            assertTrue("Read bytes were not as expected: " + Arrays.toString(readBytes), Arrays.equals(myBytes, readBytes));
            assertEquals("Unexpected char value", myChar, receivedBytesMessage.readChar());
            assertEquals("Unexpected double value", myDouble, receivedBytesMessage.readDouble(), 0.0);
            assertEquals("Unexpected float value", myFloat, receivedBytesMessage.readFloat(), 0.0);
            assertEquals("Unexpected int value", myInt, receivedBytesMessage.readInt());
            assertEquals("Unexpected long value", myLong, receivedBytesMessage.readLong());
            assertEquals("Unexpected short value", myShort, receivedBytesMessage.readShort());
            assertEquals("Unexpected UTF value", myUTF, receivedBytesMessage.readUTF());

            // reset and read the first item, leaving message marker in the middle of its content
            receivedBytesMessage.reset();
            assertEquals("Unexpected boolean value after reset", myBool, receivedBytesMessage.readBoolean());

            // Send the received message back to the test peer and have it check the result is as expected
            testPeer.expectAttach().ofSender().respond();
            testPeer.remoteFlow().withLinkCredit(10).queue();

            MessageProducer producer = session.createProducer(queue);

            HeaderMatcher headersMatcher = new HeaderMatcher(true);
            MessageAnnotationsMatcher msgAnnotationsMatcher = new MessageAnnotationsMatcher(true);
            PropertiesMatcher propsMatcher = new PropertiesMatcher(true);
            propsMatcher.withContentType(AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE.toString());
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedDataMatcher(bytesPayload));

            testPeer.expectTransfer().withPayload(messageMatcher).accept();

            producer.send(receivedBytesMessage);

            testPeer.expectClose().respond();
            connection.close();

            testPeer.waitForScriptToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testGetBodyBytesMessageFailsWhenWrongTypeRequested() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin().respond();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            final byte[] expectedContent = "expectedContent".getBytes();

            testPeer.expectAttach().ofReceiver().respond();
            testPeer.expectFlow().withLinkCredit(notNullValue());
            testPeer.remoteTransfer().withProperties().withContentType(AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE.toString())
                                     .also()
                                     .withMessageAnnotations().withAnnotation(AmqpMessageSupport.JMS_MSG_TYPE.toString(), AmqpMessageSupport.JMS_BYTES_MESSAGE)
                                     .also()
                                     .withBody().withData(expectedContent)
                                     .also()
                                     .withDeliveryId(0).queue();
            testPeer.remoteTransfer().withProperties().withContentType(AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE.toString())
                                     .also()
                                     .withMessageAnnotations().withAnnotation(AmqpMessageSupport.JMS_MSG_TYPE.toString(), AmqpMessageSupport.JMS_BYTES_MESSAGE)
                                     .also()
                                     .withBody().withData(expectedContent)
                                     .also()
                                     .withDeliveryId(1).queue();
            testPeer.expectDisposition().withState().accepted().withSettled(true);
            testPeer.expectDisposition().withState().accepted().withSettled(true);

            MessageConsumer messageConsumer = session.createConsumer(queue);

            Message readMsg1 = messageConsumer.receive(1000);
            Message readMsg2 = messageConsumer.receive(1000);

            testPeer.waitForScriptToComplete(3000);

            assertNotNull(readMsg1);
            assertNotNull(readMsg2);

            try {
                readMsg1.getBody(String.class);
                fail("Should have thrown MessageFormatException");
            } catch (MessageFormatException mfe) {
            }

            try {
                readMsg2.getBody(Map.class);
                fail("Should have thrown MessageFormatException");
            } catch (MessageFormatException mfe) {
            }

            byte[] received1 = readMsg1.getBody(byte[].class);
            byte[] received2 = (byte[]) readMsg2.getBody(Object.class);

            assertTrue(Arrays.equals(expectedContent, received1));
            assertTrue(Arrays.equals(expectedContent, received2));

            testPeer.expectClose().respond();

            connection.close();

            testPeer.waitForScriptToComplete(3000);
        }
    }

    /**
     * Test that a message received from the test peer with an AmqpValue section containing
     * Binary and no content type is returned as a BytesMessage, verify it gives the
     * expected data values when read, and when sent to the test peer it results in an
     * AMQP message containing a data body section and content type of
     * {@link AmqpMessageSupport#OCTET_STREAM_CONTENT_TYPE}
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test(timeout = 20000)
    public void testReceiveBytesMessageWithAmqpValueAndResendResultsInData() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin().respond();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");

            // Prepare an AMQP message for the test peer to send, containing an amqp-value
            // body section populated with expected bytes for use as a JMS BytesMessage,
            // and do not set content type, or the message type annotation

            boolean myBool = true;
            byte myByte = 4;
            byte[] myBytes = "myBytes".getBytes();
            char myChar = 'd';
            double myDouble = 1234567890123456789.1234;
            float myFloat = 1.1F;
            int myInt = Integer.MAX_VALUE;
            long myLong = Long.MAX_VALUE;
            short myShort = 25;
            String myUTF = "myString";

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            dos.writeBoolean(myBool);
            dos.writeByte(myByte);
            dos.write(myBytes);
            dos.writeChar(myChar);
            dos.writeDouble(myDouble);
            dos.writeFloat(myFloat);
            dos.writeInt(myInt);
            dos.writeLong(myLong);
            dos.writeShort(myShort);
            dos.writeUTF(myUTF);

            byte[] bytesPayload = baos.toByteArray();

            testPeer.expectAttach().ofReceiver().respond();
            testPeer.expectFlow().withLinkCredit(notNullValue());
            testPeer.remoteTransfer().withBody().withValue(bytesPayload)
                                     .also()
                                     .withDeliveryId(0).queue();
            testPeer.expectDisposition().withState().accepted().withSettled(true);

            // receive the message from the test peer
            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);
            testPeer.waitForScriptToComplete(3000);

            // verify the content is as expected
            assertNotNull("Message was not received", receivedMessage);
            assertTrue("Message was not a BytesMessage", receivedMessage instanceof BytesMessage);
            BytesMessage receivedBytesMessage = (BytesMessage) receivedMessage;

            assertEquals("Unexpected boolean value", myBool, receivedBytesMessage.readBoolean());
            assertEquals("Unexpected byte value", myByte, receivedBytesMessage.readByte());
            byte[] readBytes = new byte[myBytes.length];
            assertEquals("Did not read the expected number of bytes", myBytes.length, receivedBytesMessage.readBytes(readBytes));
            assertTrue("Read bytes were not as expected: " + Arrays.toString(readBytes), Arrays.equals(myBytes, readBytes));
            assertEquals("Unexpected char value", myChar, receivedBytesMessage.readChar());
            assertEquals("Unexpected double value", myDouble, receivedBytesMessage.readDouble(), 0.0);
            assertEquals("Unexpected float value", myFloat, receivedBytesMessage.readFloat(), 0.0);
            assertEquals("Unexpected int value", myInt, receivedBytesMessage.readInt());
            assertEquals("Unexpected long value", myLong, receivedBytesMessage.readLong());
            assertEquals("Unexpected short value", myShort, receivedBytesMessage.readShort());
            assertEquals("Unexpected UTF value", myUTF, receivedBytesMessage.readUTF());

            // reset and read the first item, leaving message marker in the middle of its content
            receivedBytesMessage.reset();
            assertEquals("Unexpected boolean value after reset", myBool, receivedBytesMessage.readBoolean());

            // Send the received message back to the test peer and have it check the result is as expected
            testPeer.expectAttach().ofSender().respond();
            testPeer.remoteFlow().withLinkCredit(2).queue().afterDelay(1);

            MessageProducer producer = session.createProducer(queue);

            HeaderMatcher headersMatcher = new HeaderMatcher(true);
            MessageAnnotationsMatcher msgAnnotationsMatcher = new MessageAnnotationsMatcher(true);
            msgAnnotationsMatcher.withEntry(AmqpMessageSupport.JMS_MSG_TYPE.toString(), equalTo(AmqpMessageSupport.JMS_BYTES_MESSAGE));
            PropertiesMatcher propsMatcher = new PropertiesMatcher(true);
            propsMatcher.withContentType(AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE.toString());
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propsMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedDataMatcher(bytesPayload));

            testPeer.expectTransfer().withPayload(messageMatcher).accept();

            producer.send(receivedBytesMessage);

            testPeer.expectClose().respond();
            connection.close();

            testPeer.waitForScriptToComplete(3000);
        }
    }

    @Test(timeout = 20000)
    public void testAsyncSendDoesNotMarksBytesMessageReadOnly() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setSendTimeout(15000);

            testPeer.expectBegin().respond();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            BytesMessage message = session.createBytesMessage();

            // Expect the producer to attach and grant it some credit, it should send
            // a transfer which we will not send any response so that we can check that
            // the inflight message is read-only
            testPeer.expectAttach().ofSender().respond();
            testPeer.remoteFlow().withLinkCredit(1).queue();
            testPeer.expectTransfer().withNonNullPayload();
            testPeer.expectClose().respond();

            MessageProducer producer = session.createProducer(queue);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            try {
                producer.send(message);
            } catch (Throwable error) {
                fail("Send should not fail for async send.");
            }

            try {
                message.setJMSCorrelationID("test");
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }
            try {
                message.setJMSCorrelationIDAsBytes(new byte[]{});
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }
            try {
                message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }
            try {
                message.setJMSDestination(queue);
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }
            try {
                message.setJMSExpiration(0);
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }
            try {
                message.setJMSMessageID(queueName);
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }
            try {
                message.setJMSPriority(0);
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }
            try {
                message.setJMSRedelivered(false);
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }
            try {
                message.setJMSReplyTo(queue);
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }
            try {
                message.setJMSTimestamp(0);
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }
            try {
                message.setJMSType(queueName);
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }
            try {
                message.setStringProperty("test", "test");
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to set properties on inflight message");
            }
            try {
                message.clearBody();
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to clear on inflight message");
            }
            try {
                message.writeBoolean(true);
            } catch (MessageNotWriteableException mnwe) {
                fail("Should be able to write to on inflight message");
            }

            connection.close();

            testPeer.waitForScriptToComplete(1000);
        }
    }

    @Test(timeout = 20000)
    public void testAsyncCompletionSendMarksBytesMessageReadOnly() throws Exception {
        try (ProtonTestServer testPeer = new ProtonTestServer();) {
            JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer);
            connection.setSendTimeout(15000);

            testPeer.expectBegin().respond();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = "myQueue";
            Queue queue = session.createQueue(queueName);

            BytesMessage message = session.createBytesMessage();

            // Expect the producer to attach and grant it some credit, it should send
            // a transfer which we will not send any response so that we can check that
            // the inflight message is read-only
            testPeer.expectAttach().ofSender().respond();
            testPeer.remoteFlow().withLinkCredit(1).queue();
            testPeer.expectTransfer().withNonNullPayload();
            testPeer.expectClose().respond();

            MessageProducer producer = session.createProducer(queue);
            TestJmsCompletionListener listener = new TestJmsCompletionListener();

            try {
                producer.send(message, listener);
            } catch (Throwable error) {
                fail("Send should not fail for async.");
            }

            try {
                message.setJMSCorrelationID("test");
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setJMSCorrelationIDAsBytes(new byte[]{});
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setJMSDestination(queue);
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setJMSExpiration(0);
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setJMSMessageID(queueName);
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setJMSPriority(0);
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setJMSRedelivered(false);
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setJMSReplyTo(queue);
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setJMSTimestamp(0);
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setJMSType(queueName);
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.setStringProperty("test", "test");
                fail("Should not be able to set properties on inflight message");
            } catch (MessageNotWriteableException mnwe) {}
            try {
                message.writeBoolean(true);
                fail("Message should not be writable after a send.");
            } catch (MessageNotWriteableException mnwe) {}

            connection.close();

            testPeer.waitForScriptToComplete(1000);
        }
    }

    private class TestJmsCompletionListener implements CompletionListener {

        @Override
        public void onCompletion(Message message) {
        }

        @Override
        public void onException(Message message, Exception exception) {
        }
    }
}
