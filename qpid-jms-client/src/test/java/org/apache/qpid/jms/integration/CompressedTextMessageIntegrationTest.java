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

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.ACCEPT_ENCODING;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.DEFLATE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.AMQP_SPEC_MESSAGE_FORMAT;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MSG_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_TEXT_MESSAGE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.charset.StandardCharsets;
import java.util.Map;

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
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

/**
 * Test for content encoding of compress bytes messages
 */
@Timeout(20)
public class CompressedTextMessageIntegrationTest extends CompressedMessageTestCase {

    private final IntegrationTestFixture testFixture = new IntegrationTestFixture();

    private static final int STRING_SIZE = 1024;

    private String uncompressedContent;

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        final StringBuilder uncompressedContentBuffer = new StringBuilder();

        for (int i = 0; i < STRING_SIZE; ++i) {
            uncompressedContentBuffer.append('a');
        }

        uncompressedContent = uncompressedContentBuffer.toString();

        super.setUp(testInfo);
    }

    @Test
    public void testSendMapMessageWithContent() throws Exception {
       final String compressionConfig = "?jms.compressionPolicy.compressAll=true";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final JmsConnection connection = (JmsConnection) testFixture.establishConnecton(testPeer, compressionConfig);

            connection.setMessageIDPolicy(messageIDPolicy);

            testPeer.expectBegin();

            final Map<Symbol, Object> remoteAttachProperties = Map.of(ACCEPT_ENCODING, DEFLATE);
            testPeer.expectSenderAttach(notNullValue(), notNullValue(), false, false, false, false, 0, 1, null, null, null, null, null, remoteAttachProperties);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(getTestName());
            MessageProducer producer = session.createProducer(queue);
            producer.setDisableMessageTimestamp(true); // Remove this from output properties as we can't match on it

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            msgAnnotationsMatcher.withEntry(AmqpMessageSupport.JMS_MSG_TYPE, equalTo(AmqpMessageSupport.JMS_TEXT_MESSAGE));
            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            propertiesMatcher.withContentEncoding(equalTo(Symbol.valueOf(DEFLATE)));
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propertiesMatcher);
            messageMatcher.setMessageContentMatcher(new EncodedDataMatcher(encodeAmqpValueContent(uncompressedContent)));

            testPeer.expectTransfer(messageMatcher);

            final TextMessage message = session.createTextMessage(uncompressedContent.toString());

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

            final Map<Symbol, Object> remoteAttachProperties = Map.of(ACCEPT_ENCODING, DEFLATE);
            testPeer.expectSenderAttach(notNullValue(), notNullValue(), false, false, false, false, 0, 1, null, null, null, null, null, remoteAttachProperties);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(getTestName());
            MessageProducer producer = session.createProducer(queue);

            MessageHeaderSectionMatcher headersMatcher = new MessageHeaderSectionMatcher(true).withDurable(equalTo(true));
            MessageAnnotationsSectionMatcher msgAnnotationsMatcher = new MessageAnnotationsSectionMatcher(true);
            msgAnnotationsMatcher.withEntry(AmqpMessageSupport.JMS_MSG_TYPE, equalTo(AmqpMessageSupport.JMS_TEXT_MESSAGE));
            MessagePropertiesSectionMatcher propertiesMatcher = new MessagePropertiesSectionMatcher(true);
            TransferPayloadCompositeMatcher messageMatcher = new TransferPayloadCompositeMatcher();
            messageMatcher.setHeadersMatcher(headersMatcher);
            messageMatcher.setMessageAnnotationsMatcher(msgAnnotationsMatcher);
            messageMatcher.setPropertiesMatcher(propertiesMatcher);

            testPeer.expectTransfer(messageMatcher, AMQP_SPEC_MESSAGE_FORMAT);

            producer.send(session.createTextMessage());

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

            // JMS Message carrying compressed payload of a JMS BytesMessage
            final PropertiesDescribedType properties = new PropertiesDescribedType();
            properties.setContentEncoding(Symbol.valueOf(DEFLATE));
            final MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(JMS_MSG_TYPE.toString(), JMS_TEXT_MESSAGE);
            final DescribedType dataContent = new DataDescribedType(encodeAmqpValueContent(uncompressedContent));

            final Matcher<?> receiverPropertiesMatcher = hasEntry(ACCEPT_ENCODING, DEFLATE);
            testPeer.expectReceiverAttach(notNullValue(), notNullValue(), null, null, null, null, receiverPropertiesMatcher, null);
            testPeer.expectLinkFlowRespondWithTransfer(null, msgAnnotations, properties, null, dataContent, false);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(getTestName());
            MessageConsumer consumer = session.createConsumer(queue);
            Message receivedMessage = consumer.receive(3_000);

            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof TextMessage);
            TextMessage streamMessage = (TextMessage) receivedMessage;
            assertEquals(uncompressedContent, streamMessage.getText());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test
    public void testReceiveMinimalistCompressedMessageWithAmqpValue() throws Exception {
        doTestReceiveMinimalistCompressedMessage(encodeAmqpValueContent(uncompressedContent), uncompressedContent);
    }

    @Disabled("Cannot yet detect intended message type")
    @Test
    public void testReceiveMinimalistCompressedMessageWithData() throws Exception {
        doTestReceiveMinimalistCompressedMessage(encodeDataContent(uncompressedContent.getBytes(StandardCharsets.UTF_8)), uncompressedContent);
    }

    @Disabled("Cannot yet detect intended message type")
    @Test
    public void testReceiveMinimalistCompressedMessageEmptyBinary() throws Exception {
        doTestReceiveMinimalistCompressedMessage(new Binary(new byte[0]), null);
    }

    private void doTestReceiveMinimalistCompressedMessage(Binary dataPayload, String expectedString) throws Exception {
        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final Connection connection = testFixture.establishConnecton(testPeer);

            testPeer.expectBegin();

            final PropertiesDescribedType properties = new PropertiesDescribedType();
            properties.setContentType(Symbol.valueOf("tesxt/plain"));
            properties.setContentEncoding(Symbol.valueOf(DEFLATE));
            final DescribedType dataContent = new DataDescribedType(dataPayload);

            final Matcher<?> receiverPropertiesMatcher = hasEntry(ACCEPT_ENCODING, DEFLATE);
            testPeer.expectReceiverAttach(notNullValue(), notNullValue(), null, null, null, null, receiverPropertiesMatcher, null);
            testPeer.expectLinkFlowRespondWithTransfer(null, null, properties, null, dataContent, false);
            testPeer.expectDispositionThatIsAcceptedAndSettled();

            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(getTestName());
            MessageConsumer consumer = session.createConsumer(queue);
            Message receivedMessage = consumer.receive(3_000);

            testPeer.waitForAllHandlersToComplete(3000);

            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof TextMessage);
            TextMessage streamMessage = (TextMessage) receivedMessage;
            assertEquals(expectedString, streamMessage.getText());

            testPeer.expectClose();
            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }

    @Test
    public void testReceiveTextMessageUsingDataSectionWithContentTypeTextPlainNoTypeAnnotation() throws Exception {
        String expectedString = "expectedContent";
        final byte[] sentBytes = expectedString.getBytes(StandardCharsets.UTF_8);

        doReceiveTextMessageUsingDataSectionTestImpl("text/plain", sentBytes, expectedString);
    }

    @Test
    public void testReceiveTextMessageUsingDataSectionWithContentTypeTextPlainCharsetUtf8NoTypeAnnotation() throws Exception {
        String expectedString = "expectedContent";
        final byte[] sentBytes = expectedString.getBytes(StandardCharsets.UTF_8);

        doReceiveTextMessageUsingDataSectionTestImpl("text/plain;charset=utf-8", sentBytes, expectedString);
    }

    @Test
    public void testReceiveTextMessageUsingDataSectionWithContentTypeTextPlainCharsetUtf16NoTypeAnnotation() throws Exception {
        String expectedString = "expectedContent";
        final byte[] sentBytes = expectedString.getBytes(StandardCharsets.UTF_16);

        doReceiveTextMessageUsingDataSectionTestImpl("text/plain;charset=utf-16", sentBytes, expectedString);
    }

    @Test
    public void testReceiveTextMessageUsingDataSectionWithContentTypeTextOtherNoTypeAnnotation() throws Exception {
        String expectedString = "expectedContent";
        final byte[] sentBytes = expectedString.getBytes(StandardCharsets.UTF_8);

        doReceiveTextMessageUsingDataSectionTestImpl("text/other", sentBytes, expectedString);
    }

    @Test
    public void testReceiveTextMessageUsingDataSectionWithContentTypeApplicationJsonNoTypeAnnotation() throws Exception {
        String expectedString = "expectedContent";
        final byte[] sentBytes = expectedString.getBytes(StandardCharsets.UTF_8);

        doReceiveTextMessageUsingDataSectionTestImpl("application/json", sentBytes, expectedString);
    }

    @Test
    public void testReceiveTextMessageUsingDataSectionWithContentTypeApplicationXmlNoTypeAnnotation() throws Exception {
        String expectedString = "expectedContent";
        final byte[] sentBytes = expectedString.getBytes(StandardCharsets.UTF_8);

        doReceiveTextMessageUsingDataSectionTestImpl("application/xml", sentBytes, expectedString);
    }

    private void doReceiveTextMessageUsingDataSectionTestImpl(String contentType, byte[] sentBytes, String expectedString) throws Exception {
        final String compressionConfig = "?jms.compressionPolicy.compressAll=true";

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final Connection connection = testFixture.establishConnecton(testPeer, compressionConfig);
            connection.start();

            testPeer.expectBegin();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(getTestName());

            final PropertiesDescribedType properties = new PropertiesDescribedType();
            properties.setContentType(Symbol.valueOf(contentType));
            properties.setContentEncoding(Symbol.valueOf(DEFLATE));
            final MessageAnnotationsDescribedType msgAnnotations = new MessageAnnotationsDescribedType();
            msgAnnotations.setSymbolKeyedAnnotation(JMS_MSG_TYPE.toString(), JMS_TEXT_MESSAGE);
            final DescribedType dataContent = new DataDescribedType(encodeDataContent(sentBytes));

            testPeer.expectReceiverAttach();
            testPeer.expectLinkFlowRespondWithTransfer(null, null, properties, null, dataContent, false);
            testPeer.expectDispositionThatIsAcceptedAndSettled();
            testPeer.expectClose();

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message receivedMessage = messageConsumer.receive(3000);

            assertNotNull(receivedMessage);
            assertTrue(receivedMessage instanceof TextMessage);
            String text = ((TextMessage) receivedMessage).getText();

            assertEquals(expectedString, text);

            assertTrue(receivedMessage.isBodyAssignableTo(String.class));
            assertTrue(receivedMessage.isBodyAssignableTo(Object.class));
            assertFalse(receivedMessage.isBodyAssignableTo(Boolean.class));
            assertFalse(receivedMessage.isBodyAssignableTo(byte[].class));

            assertNotNull(receivedMessage.getBody(Object.class));
            assertNotNull(receivedMessage.getBody(String.class));
            try {
                receivedMessage.getBody(byte[].class);
                fail("Cannot read TextMessage with this type.");
            } catch (MessageFormatException mfe) {
            }

            connection.close();

            testPeer.waitForAllHandlersToComplete(3000);
        }
    }
}
