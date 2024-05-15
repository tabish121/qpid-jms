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

import org.apache.qpid.jms.JmsDestination;

import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

/**
 * Interface for building policy objects that control when a MessageProducer will send
 * messages with the body of the message compressed.
 */
public interface JmsCompressionPolicy {

    JmsCompressionPolicy copy();

    /**
     * Determines when a {@link MessageProducer} is created if it should enable or disable
     * AMQP JMS message compression for that producer. Enabling AMQP JMS compression is not
     * a guarantee that compression will occur as the remote needs to agree that it supports
     * the feature.
     * <p>
     * Called when a JMS {@link MessageProducer} is being created by a {@link Session}.
     *
     * @param session
     *      the {@link Session} from which the producer is being created.
     * @param destination
     *      the destination that the Message will be sent to.
     *
     * @return true if the producer should send messages with compressed bodies.
     */
    boolean isCompressionTarget(Session session, JmsDestination destination);

    /**
     * Determines when a {@link Message} that was sent should have its body compressed
     * if the remote indicated that it supports AMQP JMS message compression and the
     * producer was created with JMS AMQP message compression enabled as per the policy.
     * This filtering method allows some {@link Message} instances to be opted out of
     * compression even though the producer that is sending it has compression enabled.
     * <p>
     * Called when a message is sent from any producer that has message compression enabled
     * and the remote indicated such support is provided.
     *
     * @param producer
     *      the {@link MessageProducer} that initiated the send.
     * @param destination
     *      the destination that the Message will be sent to.
     * @param message
     * 		the {@link Message} being sent.
     *
     * @return true if the message should be sent with a compressed body.
     */
    default boolean isCompressionTarget(MessageProducer producer, JmsDestination destination, Message message) {
        return true; // defaults to compress all if producer is compressing.
    }
}
