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

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.DELAYED_DELIVERY;
import static org.apache.qpid.jms.provider.amqp.message.AmqpJmsCompressedMessageConstants.AMQP_JMS_COMPRESSION_AWARE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpJmsCompressedMessageConstants.COMPRESSED_BYTES_MESSAGE_FORMAT;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.AMQP_SPEC_MESSAGE_FORMAT;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_BYTES_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MSG_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.DataDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.MessageAnnotationsDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.PropertiesDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageAnnotationsSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessageHeaderSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.MessagePropertiesSectionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.types.EncodedDataMatcher;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;

/**
 * Test for content encoding of compress bytes messages
 */
@Timeout(20)
public class CompressedBytesMessageIntegrationTest extends CompressedMessageTestCase {

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    private static final int PAYLOAD_SIZE = 1024;
    private static final int RANDOM_INITIAL_BYTES = 256; // Hard to compress random preamble

    private byte[] uncompressedContent;

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        final byte[] testPayload = new byte[PAYLOAD_SIZE];
        final byte[] preamble = new byte[RANDOM_INITIAL_BYTES];
        final Random random = new Random(System.currentTimeMillis());

        random.nextBytes(preamble);

        System.arraycopy(preamble, 0, testPayload, 0, RANDOM_INITIAL_BYTES);

        // Easy to compress message body should result is large savings.
        for (int i = RANDOM_INITIAL_BYTES; i < PAYLOAD_SIZE; ++i) {
            testPayload[i] = 65;
        }

        uncompressedContent = testPayload;

        super.setUp(testInfo);
    }

    @Test
    public void testSendCompressedMessageWithContent() throws Exception {
       final String compressionConfig = "?jms.compressionPolicy.compressAll=true";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer, compressionConfig);

            connection.setMessageIDPolicy(messageIDPolicy);

            testPeer.expectBegin();

            // Desire and offer message compression support so the message should be compressed if policy enables it
            Matcher<Symbol[]> desiredCapabilitiesMatcher = arrayContaining(new Symbol[] { DELAYED_DELIVERY, AMQP_JMS_COMPRESSION_AWARE });
            Symbol[] offeredCapabilities = new Symbol[] { DELAYED_DELIVERY, AMQP_JMS_COMPRESSION_AWARE };
            testPeer.expectSenderAttach(notNullValue(), notNullValue(), false, false, false, false, 0, 1, null, null, desiredCapabilitiesMatcher, offeredCapabilities);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(getTestName());
            MessageProducer producer = session.createProducer(queue);
            producer.setDisableMessageTimestamp(true); // Remove this from output properties as we can't match on it

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            msgAnnotationsMatcher.withEntry(AmqpMessageSupport.JMS_MSG_TYPE, equalTo(AmqpMessageSupport.JMS_BYTES_MESSAGE));
            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withContentType(equalTo(AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedDataMatcher(encodeDataContent(uncompressedContent, 1, OCTET_STREAM_CONTENT_TYPE.toString())));

            testPeer.expectTransfer(messageMatcher, COMPRESSED_BYTES_MESSAGE_FORMAT);

            final BytesMessage message = session.createBytesMessage();

            message.writeBytes(uncompressedContent, 0, uncompressedContent.length);

            producer.send(message);

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test
    public void testSendEmptyMessageWithCompressionEnabled() throws Exception {
        final String compressionConfig = "?jms.compressionPolicy.compressAll=true";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final Connection connection = testFixture.establishConnecton(testPeer, compressionConfig);

            testPeer.expectBegin();

            // Desire and offer message compression support so the message should be compressed if policy enables it
            Matcher<Symbol[]> desiredCapabilitiesMatcher = arrayContaining(new Symbol[] { DELAYED_DELIVERY, AMQP_JMS_COMPRESSION_AWARE });
            Symbol[] offeredCapabilities = new Symbol[] { DELAYED_DELIVERY, AMQP_JMS_COMPRESSION_AWARE };
            testPeer.expectSenderAttach(notNullValue(), notNullValue(), false, false, false, false, 0, 1, null, null, desiredCapabilitiesMatcher, offeredCapabilities);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageProducer producer = session.createProducer(queue);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            msgAnnotationsMatcher.withEntry(AmqpMessageSupport.JMS_MSG_TYPE, equalTo(AmqpMessageSupport.JMS_BYTES_MESSAGE));
            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withContentType(equalTo(AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propertiesMatcher);

            testPeer.expectTransfer(messageMatcher, AMQP_SPEC_MESSAGE_FORMAT);

            producer.send(session.createBytesMessage());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test
    public void testReceiveCompressedMessageWithContent() throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();

            // Offer message compression support and expect the remote to send it in desired capabilities
            Matcher<Symbol[]> offeredCapabilitiesMatcher = arrayContaining(new Symbol[] { AMQP_JMS_COMPRESSION_AWARE });
            Symbol[] desiredCapabilities = new Symbol[] { AMQP_JMS_COMPRESSION_AWARE };

            // JMS BytesMessage carrying compressed payload of a JMS BytesMessage
            final PropertiesDescribedType properties = new PropertiesDescribedType();
            properties.setContentType(OCTET_STREAM_CONTENT_TYPE);
            final MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(JMS_MSG_TYPE.toString(), JMS_BYTES_MESSAGE);
            final DescribedType dataContent = new DataDescribedType(encodeDataContent(uncompressedContent, 1, OCTET_STREAM_CONTENT_TYPE.toString()));

            testPeer.expectReceiverAttach(notNullValue(), notNullValue(), null, null, offeredCapabilitiesMatcher, desiredCapabilities);
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, properties, null, dataContent, false, COMPRESSED_BYTES_MESSAGE_FORMAT);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("myQueue");
            MessageConsumer consumer = session.createConsumer(queue);
            Message receivedMessage = consumer.receive(3_000);

            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof BytesMessage);
            BytesMessage bytesMessage = (BytesMessage) receivedMessage;
            assertEquals(uncompressedContent.length, bytesMessage.getBodyLength());
            byte[] recievedContent = new byte[uncompressedContent.length];
            int readBytes = bytesMessage.readBytes(recievedContent);
            assertEquals(recievedContent.length, readBytes);
            assertTrue(Arrays.equals(uncompressedContent, recievedContent));
            assertEquals("ID:AMQP_ULONG:1", bytesMessage.getJMSMessageID());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test
    public void testReceiveBytesMessageUsingDataSectionWithContentTypeOctectStream() throws Exception {
        doReceiveBasicBytesMessageUsingDataSectionTestImpl(OCTET_STREAM_CONTENT_TYPE, true);
    }

    @Test
    public void testReceiveBytesMessageUsingDataSectionWithContentTypeOctectStreamNoTypeAnnotation() throws Exception {
        doReceiveBasicBytesMessageUsingDataSectionTestImpl(OCTET_STREAM_CONTENT_TYPE, false);
    }

    @Test
    public void testReceiveBasicBytesMessageUsingDataSectionWithContentTypeEmptyNoTypeAnnotation() throws Exception {
        doReceiveBasicBytesMessageUsingDataSectionTestImpl(Symbol.valueOf(""), false);
    }

    @Test
    public void testReceiveBasicBytesMessageUsingDataSectionWithContentTypeUnknownNoTypeAnnotation() throws Exception {
        doReceiveBasicBytesMessageUsingDataSectionTestImpl(Symbol.valueOf("type/unknown"), false);
    }

    @Test
    public void testReceiveBasicBytesMessageUsingDataSectionWithContentTypeNotSetNoTypeAnnotation() throws Exception {
        doReceiveBasicBytesMessageUsingDataSectionTestImpl(null, false);
    }

    private void doReceiveBasicBytesMessageUsingDataSectionTestImpl(Symbol contentType, boolean typeAnnotation) throws JMSException, InterruptedException, Exception, IOException {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Connection connection = testFixture.establishConnecton(testPeer);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(getTestName());

            // Offer message compression support and expect the remote to send it in desired capabilities
            Matcher<Symbol[]> offeredCapabilitiesMatcher = arrayContaining(new Symbol[] { AMQP_JMS_COMPRESSION_AWARE });
            Symbol[] desiredCapabilities = new Symbol[] { AMQP_JMS_COMPRESSION_AWARE };

            // JMS BytesMessage carrying compressed payload of a JMS BytesMessage
            final MessageAnnotationsDescribedType msgAnnotations;
            if (typeAnnotation) {
                msgAnnotations = new MessageAnnotationsDescribedType();
                msgAnnotations.setSymbolKeyedAnnotation(JMS_MSG_TYPE.toString(), JMS_BYTES_MESSAGE);
            } else {
                msgAnnotations = null;
            }

            final DescribedType dataContent = new DataDescribedType(encodeDataContent(uncompressedContent, 1, contentType == null ? null : contentType.toString()));

            testPeer.expectReceiverAttach(notNullValue(), notNullValue(), null, null, offeredCapabilitiesMatcher, desiredCapabilities);
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, null, null, dataContent, false, COMPRESSED_BYTES_MESSAGE_FORMAT);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);
            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof BytesMessage);
            BytesMessage bytesMessage = (BytesMessage) receivedMessage;
            assertEquals(uncompressedContent.length, bytesMessage.getBodyLength());
            byte[] recievedContent = new byte[uncompressedContent.length];
            int readBytes = bytesMessage.readBytes(recievedContent);
            assertEquals(recievedContent.length, readBytes);
            assertTrue(Arrays.equals(uncompressedContent, recievedContent));
            assertEquals("ID:AMQP_ULONG:1", bytesMessage.getJMSMessageID());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }
}
