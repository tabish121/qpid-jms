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

import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL;
import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL;
import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.QUEUE_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.TEMP_QUEUE_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.TEMP_TOPIC_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.TOPIC_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.LEGACY_QUEUE_ATTRIBUTE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.LEGACY_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.LEGACY_TEMPORARY_ATTRIBUTE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.LEGACY_TOPIC_ATTRIBUTE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.LEGACY_TO_TYPE_MSG_ANNOTATION_SYMBOL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsTemporaryQueue;
import org.apache.qpid.jms.JmsTemporaryTopic;
import org.apache.qpid.jms.JmsTopic;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;
import org.junit.Test;
import org.mockito.Mockito;

public class AmqpDestinationHelperTest {
    private static final String LEGACY_TEMP_QUEUE_ATTRIBUTES = LEGACY_QUEUE_ATTRIBUTE + "," + LEGACY_TEMPORARY_ATTRIBUTE;
    private static final String LEGACY_TEMP_TOPIC_ATTRIBUTES = LEGACY_TOPIC_ATTRIBUTE + "," + LEGACY_TEMPORARY_ATTRIBUTE;

    //========================================================================//
    //--------------- Test getJmsDestination method --------------------------//
    //========================================================================//

    // --- general / no type annotations  --- //

    @Test
    public void testGetJmsDestinationWithNullAddressAndNullConsumerDestReturnsNull() throws Exception {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getToAddress()).thenReturn(null);

        assertNull(AmqpDestinationHelper.getJmsDestination(message, null));
    }

    @Test
    public void testGetJmsDestinationWithNullAddressWithConsumerDestReturnsSameConsumerDestObject() throws Exception {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getToAddress()).thenReturn(null);

        JmsDestination consumerDestination = new JmsQueue("ConsumerDestination");
        assertSame(consumerDestination, AmqpDestinationHelper.getJmsDestination(message, consumerDestination));

        consumerDestination = new JmsTopic("ConsumerDestination");
        assertSame(consumerDestination, AmqpDestinationHelper.getJmsDestination(message, consumerDestination));
    }

    @Test
    public void testGetJmsDestinationWithoutTypeAnnotationWithAnonymousConsumerDest() {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);

        JmsDestination consumerDestination = Mockito.mock(JmsDestination.class);
        Mockito.when(consumerDestination.getAddress()).thenReturn("ConsumerDestination");

        JmsDestination destination = AmqpDestinationHelper.getJmsDestination(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsDestinationWithoutTypeAnnotationWithQueueConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        JmsQueue consumerDestination = new JmsQueue("ConsumerDestination");

        JmsDestination destination = AmqpDestinationHelper.getJmsDestination(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }


    @Test
    public void testGetJmsDestinationWithoutTypeAnnotationWithTopicConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        JmsDestination consumerDestination = new JmsTopic("ConsumerDestination");

        JmsDestination destination = AmqpDestinationHelper.getJmsDestination(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isTopic());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsDestinationWithoutTypeAnnotationWithTempQueueConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        JmsDestination consumerDestination = new JmsTemporaryQueue("ConsumerDestination");

        JmsDestination destination = AmqpDestinationHelper.getJmsDestination(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertTrue(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsDestinationWithoutTypeAnnotationWithTempTopicConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        JmsDestination consumerDestination = new JmsTemporaryTopic("ConsumerDestination");

        JmsDestination destination = AmqpDestinationHelper.getJmsDestination(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isTopic());
        assertTrue(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    // --- new byte destination type annotations --- //

    @Test
    public void testGetJmsDestinationWithUnknownTypeAnnotationWithQueueConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL)).thenReturn((byte) 5);
        JmsQueue consumerDestination = new JmsQueue("ConsumerDestination");

        JmsDestination destination = AmqpDestinationHelper.getJmsDestination(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsDestinationWithQueueTypeAnnotationNoConsumerDestination() throws Exception {
        doGetJmsDestinationWithQueueTypeAnnotationTestImpl(null);
    }

    @Test
    public void testGetJmsDestinationWithQueueTypeAnnotationWithConsumerDestination() throws Exception {
        doGetJmsDestinationWithQueueTypeAnnotationTestImpl(new JmsTopic("ConsumerDestination"));
    }

    private void doGetJmsDestinationWithQueueTypeAnnotationTestImpl(JmsDestination consumerDestination) {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL)).thenReturn(QUEUE_TYPE);

        JmsDestination destination = AmqpDestinationHelper.getJmsDestination(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsDestinationWithTopicTypeAnnotationNoConsumerDestination() throws Exception {
        doGetJmsDestinationWithTopicTypeAnnotationTestImpl(null);
    }

    @Test
    public void testGetJmsDestinationWithTopicTypeAnnotationWithConsumerDestination() throws Exception {
        doGetJmsDestinationWithTopicTypeAnnotationTestImpl(new JmsQueue("ConsumerDestination"));
    }

    private void doGetJmsDestinationWithTopicTypeAnnotationTestImpl(JmsDestination consumerDestination) {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL)).thenReturn(TOPIC_TYPE);

        JmsDestination destination = AmqpDestinationHelper.getJmsDestination(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isTopic());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsDestinationWithTempQueueTypeAnnotationNoConsumerDestination() throws Exception {
        doGetJmsDestinationWithTempQueueTypeAnnotationTestImpl(null);
    }

    @Test
    public void testGetJmsDestinationWithTempQueueTypeAnnotationWithConsumerDestination() throws Exception {
        doGetJmsDestinationWithTempQueueTypeAnnotationTestImpl(new JmsQueue("ConsumerDestination"));
    }

    private void doGetJmsDestinationWithTempQueueTypeAnnotationTestImpl(JmsDestination consumerDestination) {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL)).thenReturn(TEMP_QUEUE_TYPE);

        JmsDestination destination = AmqpDestinationHelper.getJmsDestination(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertTrue(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsDestinationWithTempTopicTypeAnnotationNoConsumerDestination() throws Exception {
        doGetJmsDestinationWithTempTopicTypeAnnotationTestImpl(null);
    }

    @Test
    public void testGetJmsDestinationWithTempTopicTypeAnnotationWithConsumerDestination() throws Exception {
        doGetJmsDestinationWithTempTopicTypeAnnotationTestImpl(new JmsQueue("ConsumerDestination"));
    }

    private void doGetJmsDestinationWithTempTopicTypeAnnotationTestImpl(JmsDestination consumerDestination) {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL)).thenReturn(TEMP_TOPIC_TYPE);

        JmsDestination destination = AmqpDestinationHelper.getJmsDestination(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isTopic());
        assertTrue(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    // --- legacy string destination type annotations --- //

    @Test
    public void testGetJmsDestinationWithEmptyLegacyTypeAnnotationWithQueueConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(LEGACY_TO_TYPE_MSG_ANNOTATION_SYMBOL)).thenReturn("");
        JmsQueue consumerDestination = new JmsQueue("ConsumerDestination");

        JmsDestination destination = AmqpDestinationHelper.getJmsDestination(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsDestinationWithUnknownLegacyTypeAnnotationWithQueueConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(LEGACY_TO_TYPE_MSG_ANNOTATION_SYMBOL)).thenReturn("jms.queue");
        JmsQueue consumerDestination = new JmsQueue("ConsumerDestination");

        JmsDestination destination = AmqpDestinationHelper.getJmsDestination(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsDestinationWithLegacyQueueTypeAnnotationNoConsumerDestination() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(LEGACY_TO_TYPE_MSG_ANNOTATION_SYMBOL)).thenReturn(LEGACY_QUEUE_ATTRIBUTE);

        JmsDestination destination = AmqpDestinationHelper.getJmsDestination(message, null);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsDestinationWithLegacyTopicTypeAnnotationNoConsumerDestination() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(LEGACY_TO_TYPE_MSG_ANNOTATION_SYMBOL)).thenReturn(LEGACY_TOPIC_ATTRIBUTE);

        JmsDestination destination = AmqpDestinationHelper.getJmsDestination(message, null);
        assertNotNull(destination);
        assertTrue(destination.isTopic());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsDestinationWithLegacyTempQueueTypeAnnotationNoConsumerDestination() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(LEGACY_TO_TYPE_MSG_ANNOTATION_SYMBOL)).thenReturn(LEGACY_TEMP_QUEUE_ATTRIBUTES);

        JmsDestination destination = AmqpDestinationHelper.getJmsDestination(message, null);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertTrue(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsDestinationWithLegacyTempTopicTypeAnnotationNoConsumerDestination() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(LEGACY_TO_TYPE_MSG_ANNOTATION_SYMBOL)).thenReturn(LEGACY_TEMP_TOPIC_ATTRIBUTES);

        JmsDestination destination = AmqpDestinationHelper.getJmsDestination(message, null);
        assertNotNull(destination);
        assertTrue(destination.isTopic());
        assertTrue(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    //========================================================================//
    //--------------- Test getJmsReplyTo method ------------------------------//
    //========================================================================//

    // --- general / no type annotations  --- //

    @Test
    public void testGetJmsReplyToWithNullAddressAndNullConsumerDestReturnsNull() throws Exception {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getReplyToAddress()).thenReturn(null);

        assertNull(AmqpDestinationHelper.getJmsDestination(message, null));
    }

    @Test
    public void testGetJmsReplyToWithNullAddressWithConsumerDestReturnsNull() throws Exception {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getReplyToAddress()).thenReturn(null);
        JmsQueue consumerDestination = new JmsQueue("ConsumerDestination");

        assertNull(AmqpDestinationHelper.getJmsReplyTo(message, consumerDestination));
    }

    @Test
    public void testGetJmsReplyToWithoutTypeAnnotationWithQueueConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);

        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        JmsQueue consumerDestination = new JmsQueue("ConsumerDestination");

        JmsDestination destination = AmqpDestinationHelper.getJmsReplyTo(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsReplyToWithoutTypeAnnotationWithTopicConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        JmsTopic consumerDestination = new JmsTopic("ConsumerDestination");

        JmsDestination destination = AmqpDestinationHelper.getJmsReplyTo(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isTopic());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsReplyToWithoutTypeAnnotationWithTempTopicConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        JmsTemporaryTopic consumerDestination = new JmsTemporaryTopic("ConsumerDestination");

        JmsDestination destination = AmqpDestinationHelper.getJmsReplyTo(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isTopic());
        assertTrue(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsReplyToWithoutTypeAnnotationWithTempQueueConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        JmsTemporaryQueue consumerDestination = new JmsTemporaryQueue("ConsumerDestination");

        JmsDestination destination = AmqpDestinationHelper.getJmsReplyTo(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertTrue(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsReplyToWithoutTypeAnnotationWithAnonymousConsumerDest() {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);

        JmsDestination consumerDestination = Mockito.mock(JmsDestination.class);
        Mockito.when(consumerDestination.getAddress()).thenReturn("ConsumerDestination");

        JmsDestination destination = AmqpDestinationHelper.getJmsReplyTo(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    // --- new byte destination type annotations --- //

    @Test
    public void testGetJmsReplyToWithUnknownTypeAnnotationWithQueueConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL)).thenReturn((byte) 5);
        JmsQueue consumerDestination = new JmsQueue("ConsumerDestination");

        JmsDestination destination = AmqpDestinationHelper.getJmsReplyTo(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsReplToWithQueueTypeAnnotationNoConsumerDestination() throws Exception {
        doGetJmsReplToWithQueueTypeAnnotationTestImpl(null);
    }

    @Test
    public void testGetJmsReplToWithQueueTypeAnnotationWithConsumerDestination() throws Exception {
        doGetJmsReplToWithQueueTypeAnnotationTestImpl(new JmsTopic("ConsumerDestination"));
    }

    private void doGetJmsReplToWithQueueTypeAnnotationTestImpl(JmsDestination consumerDestination) {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL)).thenReturn(QUEUE_TYPE);

        JmsDestination destination = AmqpDestinationHelper.getJmsReplyTo(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsReplToWithTopicTypeAnnotationNoConsumerDestination() throws Exception {
        doGetJmsReplToWithTopicTypeAnnotationTestImpl(null);
    }

    @Test
    public void testGetJmsReplToWithTopicTypeAnnotationWithConsumerDestination() throws Exception {
        doGetJmsReplToWithTopicTypeAnnotationTestImpl(new JmsQueue("ConsumerDestination"));
    }

    private void doGetJmsReplToWithTopicTypeAnnotationTestImpl(JmsDestination consumerDestination) {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL)).thenReturn(TOPIC_TYPE);

        JmsDestination destination = AmqpDestinationHelper.getJmsReplyTo(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isTopic());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsReplToWithTempQueueTypeAnnotationNoConsumerDestination() throws Exception {
        doGetJmsReplToWithTempQueueTypeAnnotationTestImpl(null);
    }

    @Test
    public void testGetJmsReplToWithTempQueueTypeAnnotationWithConsumerDestination() throws Exception {
        doGetJmsReplToWithTempQueueTypeAnnotationTestImpl(new JmsTopic("ConsumerDestination"));
    }

    private void doGetJmsReplToWithTempQueueTypeAnnotationTestImpl(JmsDestination consumerDestination) {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL)).thenReturn(TEMP_QUEUE_TYPE);

        JmsDestination destination = AmqpDestinationHelper.getJmsReplyTo(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertTrue(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsReplToWithTempTopicTypeAnnotationNoConsumerDestination() throws Exception {
        doGetJmsReplToWithTempTopicTypeAnnotationTestImpl(null);
    }

    @Test
    public void testGetJmsReplToWithTempTopicTypeAnnotationWithConsumerDestination() throws Exception {
        doGetJmsReplToWithTempTopicTypeAnnotationTestImpl(new JmsTopic("ConsumerDestination"));
    }

    private void doGetJmsReplToWithTempTopicTypeAnnotationTestImpl(JmsDestination consumerDestination) {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL)).thenReturn(TEMP_TOPIC_TYPE);

        JmsDestination destination = AmqpDestinationHelper.getJmsReplyTo(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isTopic());
        assertTrue(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    // --- legacy string destination type annotations --- //

    @Test
    public void testGetJmsReplyToWithEmptyLegacyTypeAnnotationWithQueueConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(LEGACY_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL)).thenReturn("");
        JmsQueue consumerDestination = new JmsQueue("ConsumerDestination");

        JmsDestination destination = AmqpDestinationHelper.getJmsReplyTo(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsReplyToWithUnknownLegacyTypeAnnotationWithQueueConsumerDest() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(LEGACY_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL)).thenReturn("jms.queue");
        JmsQueue consumerDestination = new JmsQueue("ConsumerDestination");

        JmsDestination destination = AmqpDestinationHelper.getJmsReplyTo(message, consumerDestination);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsReplToWithLegacyQueueTypeAnnotationNoConsumerDestination() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(LEGACY_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL)).thenReturn(LEGACY_QUEUE_ATTRIBUTE);

        JmsDestination destination = AmqpDestinationHelper.getJmsReplyTo(message, null);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsReplToWithLegacyTopicTypeAnnotationNoConsumerDestination() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(LEGACY_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL)).thenReturn(LEGACY_TOPIC_ATTRIBUTE);

        JmsDestination destination = AmqpDestinationHelper.getJmsReplyTo(message, null);
        assertNotNull(destination);
        assertTrue(destination.isTopic());
        assertFalse(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsReplToWithLegacyTempQueueTypeAnnotationNoConsumerDestination() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(LEGACY_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL)).thenReturn(LEGACY_TEMP_QUEUE_ATTRIBUTES);

        JmsDestination destination = AmqpDestinationHelper.getJmsReplyTo(message, null);
        assertNotNull(destination);
        assertTrue(destination.isQueue());
        assertTrue(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    @Test
    public void testGetJmsReplToWithLegacyTempTopicTypeAnnotationNoConsumerDestination() throws Exception {
        String testAddress = "testAddress";
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getReplyToAddress()).thenReturn(testAddress);
        Mockito.when(message.getMessageAnnotation(LEGACY_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL)).thenReturn(LEGACY_TEMP_TOPIC_ATTRIBUTES);

        JmsDestination destination = AmqpDestinationHelper.getJmsReplyTo(message, null);
        assertNotNull(destination);
        assertTrue(destination.isTopic());
        assertTrue(destination.isTemporary());
        assertEquals(testAddress, destination.getAddress());
    }

    //========================================================================//
    //--------------- Test setToAddressFromDestination method ----------------//
    //========================================================================//

    @Test
    public void testSetToAddressFromDestinationWithNullDestination() {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpDestinationHelper.setToAddressFromDestination(message, null);
        Mockito.verify(message).setToAddress(null);
        Mockito.verify(message).removeMessageAnnotation(JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL);
    }

    @Test(expected=NullPointerException.class)
    public void testSetToAddressFromDestinationWithNullDestinationAndNullMessage() {
        AmqpDestinationHelper.setToAddressFromDestination(null, null);
    }

    @Test(expected=NullPointerException.class)
    public void testSetToAddressFromDestinationWithNullMessage() {
        JmsDestination destination = new JmsQueue("testAddress");
        AmqpDestinationHelper.setToAddressFromDestination(null, destination);
    }

    @Test
    public void testSetToAddressFromDestinationWithQueueClearsLegacyAnnotation() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsQueue("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);

        AmqpDestinationHelper.setToAddressFromDestination(message, destination);

        Mockito.verify(message).setToAddress(testAddress);
        Mockito.verify(message).removeMessageAnnotation(LEGACY_TO_TYPE_MSG_ANNOTATION_SYMBOL);
    }

    @Test
    public void testSetToAddressFromDestinationWithQueue() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsQueue("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);

        AmqpDestinationHelper.setToAddressFromDestination(message, destination);

        Mockito.verify(message).setToAddress(testAddress);
        Mockito.verify(message).removeMessageAnnotation(JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL);

        assertNull(message.getMessageAnnotations());
    }

    @Test
    public void testSetToAddressFromDestinationWithTopic() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsTopic("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);

        AmqpDestinationHelper.setToAddressFromDestination(message, destination);

        Mockito.verify(message).setToAddress(testAddress);
        Mockito.verify(message).removeMessageAnnotation(JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL);

        assertNull(message.getMessageAnnotations());
    }

    @Test
    public void testSetToAddressFromDestinationWithTempQueue() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsTemporaryQueue("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);

        AmqpDestinationHelper.setToAddressFromDestination(message, destination);

        Mockito.verify(message).setToAddress(testAddress);
        Mockito.verify(message).removeMessageAnnotation(JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL);

        assertNull(message.getMessageAnnotations());
    }

    @Test
    public void testSetToAddressFromDestinationWithTempTopic() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsTemporaryTopic("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);

        AmqpDestinationHelper.setToAddressFromDestination(message, destination);

        Mockito.verify(message).setToAddress(testAddress);
        Mockito.verify(message).removeMessageAnnotation(JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL);

        assertNull(message.getMessageAnnotations());
    }

    @Test
    public void testSetToAddressFromDestinationWithAnonymousDestination() {
        String testAddress = "testAddress";
        JmsDestination destination = Mockito.mock(JmsDestination.class);
        Mockito.when(destination.getAddress()).thenReturn(testAddress);
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);

        AmqpDestinationHelper.setToAddressFromDestination(message, destination);

        Mockito.verify(message).setToAddress(testAddress);
        Mockito.verify(message).removeMessageAnnotation(JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL);

        assertNull(message.getMessageAnnotations());
    }

    //========================================================================//
    //--------------- Test setReplyToAddressFromDestination method -----------//
    //========================================================================//

    @Test
    public void testSetReplyToAddressFromDestinationWithNullDestination() {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpDestinationHelper.setReplyToAddressFromDestination(message, null);
        Mockito.verify(message).setReplyToAddress(null);
        Mockito.verify(message).removeMessageAnnotation(JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL);
    }

    @Test(expected=NullPointerException.class)
    public void testSetReplyToAddressFromDestinationWithNullDestinationAndNullMessage() {
        AmqpDestinationHelper.setReplyToAddressFromDestination(null, null);
    }

    @Test(expected=NullPointerException.class)
    public void testSetReplyToAddressFromDestinationWithNullMessage() {
        JmsDestination destination = new JmsQueue("testAddress");
        AmqpDestinationHelper.setReplyToAddressFromDestination(null, destination);
    }

    @Test
    public void testSetReplyToAddressFromDestinationWithQueue() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsQueue("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);

        AmqpDestinationHelper.setReplyToAddressFromDestination(message, destination);

        Mockito.verify(message).setReplyToAddress(testAddress);
        Mockito.verify(message).removeMessageAnnotation(JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL);

        assertNull(message.getMessageAnnotations());
    }

    @Test
    public void testSetReplyToAddressFromDestinationWithQueueClearsLegacyAnnotation() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsQueue("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);

        AmqpDestinationHelper.setReplyToAddressFromDestination(message, destination);

        Mockito.verify(message).setReplyToAddress(testAddress);
        Mockito.verify(message).removeMessageAnnotation(LEGACY_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL);
    }

    @Test
    public void testSetReplyToAddressFromDestinationWithTopic() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsTopic("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        AmqpConnection conn = Mockito.mock(AmqpConnection.class);
        Mockito.when(message.getConnection()).thenReturn(conn);

        AmqpDestinationHelper.setReplyToAddressFromDestination(message, destination);

        Mockito.verify(message).setReplyToAddress(testAddress);
        Mockito.verify(message).removeMessageAnnotation(JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL);

        assertNull(message.getMessageAnnotations());
    }

    @Test
    public void testSetReplyToAddressFromDestinationWithTempQueue() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsTemporaryQueue("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);

        AmqpDestinationHelper.setReplyToAddressFromDestination(message, destination);

        Mockito.verify(message).setReplyToAddress(testAddress);
        Mockito.verify(message).removeMessageAnnotation(JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL);

        assertNull(message.getMessageAnnotations());
    }

    @Test
    public void testSetReplyToAddressFromDestinationWithTempTopic() {
        String testAddress = "testAddress";
        JmsDestination destination = new JmsTemporaryTopic("testAddress");
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);

        AmqpDestinationHelper.setReplyToAddressFromDestination(message, destination);

        Mockito.verify(message).setReplyToAddress(testAddress);
        Mockito.verify(message).removeMessageAnnotation(JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL);

        assertNull(message.getMessageAnnotations());
    }

    @Test
    public void testSetReplyToAddressFromDestinationWithAnonymousDestination() {
        String testAddress = "testAddress";
        JmsDestination destination = Mockito.mock(JmsDestination.class);
        Mockito.when(destination.getAddress()).thenReturn(testAddress);
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);

        AmqpDestinationHelper.setReplyToAddressFromDestination(message, destination);

        Mockito.verify(message).setReplyToAddress(testAddress);
        Mockito.verify(message).removeMessageAnnotation(JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL);
    }

    //========================================================================//
    //--------------- Test Support Methods -----------------------------------//
    //========================================================================//

    @Test
    public void testSplitAttributeWithExtraneousCommas() throws Exception {
        Set<String> set = new HashSet<String>();
        set.add(LEGACY_QUEUE_ATTRIBUTE);
        set.add(LEGACY_TEMPORARY_ATTRIBUTE);

        // test for no NPE errors.
        assertNull(AmqpDestinationHelper.splitAttributesString(null));

        // test a single comma separator produces expected set
        assertEquals(set, AmqpDestinationHelper.splitAttributesString(LEGACY_QUEUE_ATTRIBUTE + "," +
                                                 LEGACY_TEMPORARY_ATTRIBUTE));

        // test trailing comma doesn't alter produced set
        assertEquals(set, AmqpDestinationHelper.splitAttributesString(LEGACY_QUEUE_ATTRIBUTE + "," +
                                                 LEGACY_TEMPORARY_ATTRIBUTE + ","));

        // test leading comma doesn't alter produced set
        assertEquals(set, AmqpDestinationHelper.splitAttributesString("," + LEGACY_QUEUE_ATTRIBUTE + ","
                                                     + LEGACY_TEMPORARY_ATTRIBUTE));

        // test consecutive central commas don't alter produced set
        assertEquals(set, AmqpDestinationHelper.splitAttributesString(LEGACY_QUEUE_ATTRIBUTE + ",," +
                                                 LEGACY_TEMPORARY_ATTRIBUTE));

        // test consecutive trailing commas don't alter produced set
        assertEquals(set, AmqpDestinationHelper.splitAttributesString(LEGACY_QUEUE_ATTRIBUTE + "," +
                                                 LEGACY_TEMPORARY_ATTRIBUTE + ",,"));

        // test consecutive leading commas don't alter produced set
        assertEquals(set, AmqpDestinationHelper.splitAttributesString("," + LEGACY_QUEUE_ATTRIBUTE + ","
                                                     + LEGACY_TEMPORARY_ATTRIBUTE));
    }
}
