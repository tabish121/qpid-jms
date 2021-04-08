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
package org.apache.qpid.jms.provider.amqp;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.jms.IllegalStateException;

import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsConsumerInfo;
import org.apache.qpid.jms.meta.JmsProducerId;
import org.apache.qpid.jms.meta.JmsProducerInfo;
import org.apache.qpid.jms.meta.JmsSessionId;
import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.meta.JmsTransactionId;
import org.apache.qpid.jms.meta.JmsTransactionInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.amqp.builders.AmqpConsumerBuilder;
import org.apache.qpid.jms.provider.amqp.builders.AmqpProducerBuilder;
import org.apache.qpid.protonj2.engine.Receiver;
import org.apache.qpid.protonj2.engine.Sender;
import org.apache.qpid.protonj2.engine.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpSession extends AmqpAbstractEndpoint<JmsSessionInfo, Session> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpSession.class);

    private final AmqpConnection connection;
    private final AmqpTransactionContext txContext;

    public AmqpSession(AmqpConnection connection, JmsSessionInfo info, Session session) {
        super(connection.getProvider(), info, session);

        this.connection = connection;

        if (info.isTransacted()) {
            txContext = new AmqpTransactionContext(this, info);
        } else {
            txContext = null;
        }
    }

    /**
     * Perform an acknowledge of all delivered messages for all consumers active in this
     * Session.
     *
     * @param ackType
     *      controls the acknowledgement that is applied to each message.
     */
    public void acknowledge(final ACK_TYPE ackType) {
        // A consumer whose close was deferred will be closed and removed from the consumers
        // map so we must copy the entries to safely traverse the collection during this operation.
        getEndpoint().receivers().forEach(receiver -> {
            receiver.getLinkedResource(AmqpConsumer.class).acknowledge(ackType);
        });
    }

    /**
     * Perform re-send of all delivered but not yet acknowledged messages for all consumers
     * active in this Session.
     *
     * @throws Exception if an error occurs while performing the recover.
     */
    public void recover() throws Exception {
        for (Receiver receiver : getEndpoint().receivers()) {
            receiver.getLinkedResource(AmqpConsumer.class).recover();
        };
    }

    public void createProducer(JmsProducerInfo producerInfo, AsyncResult request) {
        AmqpProducerBuilder builder = new AmqpProducerBuilder(this, producerInfo);
        builder.buildEndpoint(request);
    }

    public AmqpProducer getProducer(JmsProducerInfo producerInfo) {
        final JmsProducerId producerId = producerInfo.getId();

        if (producerId.getProviderHint() instanceof AmqpProducer) {
            return (AmqpProducer) producerId.getProviderHint();
        }

        for (Sender sender : getEndpoint().senders()) {
            AmqpProducer producer = sender.getLinkedResource(AmqpProducer.class);
            if (producer.getProducerId().equals(producerId)) {
                return producer;
            }
        };

        throw new IllegalArgumentException("Cannot find producer in session with Id: " + producerId);
    }

    public void createConsumer(JmsConsumerInfo consumerInfo, AsyncResult request) {
        new AmqpConsumerBuilder(this, consumerInfo).buildEndpoint(request);
    }

    public AmqpConsumer getConsumer(JmsConsumerInfo consumerInfo) {
        JmsConsumerId consumerId = consumerInfo.getId();

        if (consumerId.getProviderHint() instanceof AmqpConsumer) {
            return (AmqpConsumer) consumerId.getProviderHint();
        }

        for (Receiver receiver : getEndpoint().receivers()) {
            AmqpConsumer consumer = receiver.getLinkedResource(AmqpConsumer.class);
            if (consumer.getConsumerId().equals(consumerId)) {
                return consumer;
            }
        };

        throw new IllegalArgumentException("Cannot find consumer in session with Id: " + consumerId);
    }

    public AmqpTransactionContext getTransactionContext() {
        return txContext;
    }

    /**
     * Begins a new Transaction using the given Transaction Id as the identifier.  The AMQP
     * binary Transaction Id will be stored in the provider hint value of the given transaction.
     *
     * @param txId
     *        The JMS Framework's assigned Transaction Id for the new TX.
     * @param request
     *        The request that will be signaled on completion of this operation.
     *
     * @throws Exception if an error occurs while performing the operation.
     */
    public void begin(JmsTransactionId txId, AsyncResult request) throws Exception {
        if (!getResourceInfo().isTransacted()) {
            throw new IllegalStateException("Non-transacted Session cannot start a TX.");
        }

        getTransactionContext().begin(txId, request);
    }

    /**
     * Commit the currently running Transaction.
     *
     * @param transactionInfo
     *        the JmsTransactionInfo describing the transaction being committed.
     * @param nextTransactionInfo
     *        the JmsTransactionInfo describing the transaction that should be started immediately.
     * @param request
     *        The request that will be signaled on completion of this operation.
     *
     * @throws Exception if an error occurs while performing the operation.
     */
    public void commit(JmsTransactionInfo transactionInfo, JmsTransactionInfo nextTransactionInfo, AsyncResult request) throws Exception {
        if (!getResourceInfo().isTransacted()) {
            throw new IllegalStateException("Non-transacted Session cannot commit a TX.");
        }

        getTransactionContext().commit(transactionInfo, nextTransactionInfo, request);
    }

    /**
     * Roll back the currently running Transaction
     *
     * @param transactionInfo
     *        the JmsTransactionInfo describing the transaction being rolled back.
     * @param nextTransactionInfo
     *        the JmsTransactionInfo describing the transaction that should be started immediately.
     * @param request
     *        The request that will be signaled on completion of this operation.
     *
     * @throws Exception if an error occurs while performing the operation.
     */
    public void rollback(JmsTransactionInfo transactionInfo, JmsTransactionInfo nextTransactionInfo, AsyncResult request) throws Exception {
        if (!getResourceInfo().isTransacted()) {
            throw new IllegalStateException("Non-transacted Session cannot rollback a TX.");
        }

        getTransactionContext().rollback(transactionInfo, nextTransactionInfo, request);
    }

    /**
     * Allows a session resource to schedule a task for future execution.
     *
     * @param task
     *      The Runnable task to be executed after the given delay.
     * @param delay
     *      The delay in milliseconds to schedule the given task for execution.
     *
     * @return a ScheduledFuture instance that can be used to cancel the task.
     */
    public ScheduledFuture<?> schedule(final Runnable task, long delay) {
        if (task == null) {
            LOG.trace("Resource attempted to schedule a null task.");
            return null;
        }

        return getProvider().getScheduler().schedule(task, delay, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void processEndpointClosed() {
        getEndpoint().receivers().forEach(receiver -> {
            try {
                if (receiver.getLinkedResource() != null) {
                    receiver.getLinkedResource(AmqpConsumer.class).processParentEndpointClosed(this);
                }
            } catch (ClassCastException cse) {
                // Unknown Consumer in the senders chain is a bit odd but could be a closed resource
                // that has not had a remote response
                LOG.trace("Skiped process close on unknown session receiver: " + receiver);
            }
        });

        getEndpoint().senders().forEach(sender -> {
            try {
                if (sender.getLinkedResource() != null) {
                    sender.getLinkedResource(AmqpProducer.class).processParentEndpointClosed(this);
                }
            } catch (ClassCastException cse) {
                // Unknown Producer in the senders chain is a bit odd but could be a closed resource
                // that has not had a remote response
                LOG.trace("Skiped process close on unknown session sender: " + sender);
            }
        });
    }

    /**
     * Call to send an error that occurs outside of the normal asynchronous processing
     * of a session resource such as a remote close etc.
     *
     * @param error
     *        The error to forward on to the Provider error event handler.
     */
    public void reportError(ProviderException error) {
        getConnection().getProvider().fireProviderException(error);
    }

    @Override
    public AmqpProvider getProvider() {
        return connection.getProvider();
    }

    public AmqpConnection getConnection() {
        return connection;
    }

    public JmsSessionId getSessionId() {
        return getResourceInfo().getId();
    }

    boolean isTransacted() {
        return getResourceInfo().isTransacted();
    }

    public boolean isTransactionInDoubt() {
        return txContext == null ? false : txContext.isTransactionInDoubt();
    }

    boolean isAsyncAck() {
        return getResourceInfo().isSendAcksAsync() || isTransacted();
    }

    @Override
    public String toString() {
        return "AmqpSession { " + getSessionId() + " }";
    }
}
