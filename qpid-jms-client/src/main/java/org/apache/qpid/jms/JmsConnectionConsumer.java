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
package org.apache.qpid.jms;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.jms.ConnectionConsumer;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.ServerSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsConsumerInfo;
import org.apache.qpid.jms.meta.JmsResource.ResourceState;
import org.apache.qpid.jms.util.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JMS Connection Consumer implementation.
 */
public class JmsConnectionConsumer implements ConnectionConsumer, JmsMessageDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(JmsConnectionConsumer.class);

    private static final long DEFAULT_DISPATCH_RETRY_DELAY = 1000;

    private final JmsConnection connection;
    private final JmsConsumerInfo consumerInfo;
    private final ServerSessionPool sessionPool;
    private final MessageQueue messageQueue;

    private final Lock stateLock = new ReentrantLock();
    private final Lock dispatchLock = new ReentrantLock();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicReference<Throwable> failureCause = new AtomicReference<>();
    private final ScheduledThreadPoolExecutor dispatcher;

    public JmsConnectionConsumer(JmsConnection connection, JmsConsumerInfo consumerInfo, MessageQueue messageQueue, ServerSessionPool sessionPool) throws JMSException {
        this.connection = connection;
        this.consumerInfo = consumerInfo;
        this.sessionPool = sessionPool;
        this.messageQueue = messageQueue;
        this.dispatcher = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {

            @Override
            public Thread newThread(Runnable runner) {
                Thread serial = new Thread(runner);
                serial.setDaemon(true);
                serial.setName(this.getClass().getSimpleName() + ":(" + consumerInfo.getId() + ")");
                return serial;
            }
        });

        // Ensure a timely shutdown for consumer close.
        dispatcher.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        dispatcher.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);

        connection.addConnectionConsumer(consumerInfo, this);
        try {
            connection.createResource(consumerInfo);
        } catch (JMSException jmse) {
            connection.removeConnectionConsumer(consumerInfo);
            throw jmse;
        }
    }

    public JmsConnectionConsumer init() throws JMSException {
        getConnection().startResource(consumerInfo);
        return this;
    }

    @Override
    public void onInboundMessage(JmsInboundMessageDispatch envelope) {
        envelope.setConsumerInfo(consumerInfo);

        stateLock.lock();
        try {
            if (envelope.isEnqueueFirst()) {
                this.messageQueue.enqueueFirst(envelope);
            } else {
                this.messageQueue.enqueue(envelope);
            }

            if (messageQueue.isRunning()) {
                try {
                    dispatcher.execute(() -> deliverNextPending());
                } catch (RejectedExecutionException rje) {
                    LOG.debug("Rejected on attempt to queue message dispatch", rje);
                }
            }
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public void close() throws JMSException {
        if (!closed.get()) {
            doClose();
        }
    }

    /**
     * Called to initiate shutdown of consumer resources and request that the remote
     * peer remove the registered producer.
     *
     * @throws JMSException if an error occurs during the consumer close operation.
     */
    protected void doClose() throws JMSException {
        shutdown();
        this.connection.destroyResource(consumerInfo);
    }

    protected void shutdown() throws JMSException {
        shutdown(null);
    }

    protected void shutdown(Throwable cause) throws JMSException {
        if (closed.compareAndSet(false, true)) {
            dispatchLock.lock();
            try {
                failureCause.set(cause);
                consumerInfo.setState(ResourceState.CLOSED);
                connection.removeConnectionConsumer(consumerInfo);
                stop(true);
                dispatcher.shutdown();
                try {
                    dispatcher.awaitTermination(connection.getCloseTimeout(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    LOG.trace("ConnectionConsumer shutdown of dispatcher was interupted");
                }
            } finally {
                dispatchLock.unlock();
            }
        }
    }

    public void start() {
        stateLock.lock();
        try {
            if (!messageQueue.isRunning()) {
                this.messageQueue.start();
                this.dispatcher.execute(new BoundedMessageDeliverTask(messageQueue.size()));
            }
        } finally {
            stateLock.unlock();
        }
    }

    public void stop() {
        stop(false);
    }

    private void stop(boolean closeMessageQueue) {
        dispatchLock.lock();
        stateLock.lock();
        try {
            if (closeMessageQueue) {
                this.messageQueue.close();
            } else {
                this.messageQueue.stop();
            }
        } finally {
            stateLock.unlock();
            dispatchLock.unlock();
        }
    }

    @Override
    public ServerSessionPool getServerSessionPool() throws JMSException {
        checkClosed();
        return sessionPool;
    }

    JmsConnection getConnection() {
        return connection;
    }

    JmsConsumerInfo getConsumerInfo() {
        return consumerInfo;
    }

    void setFailureCause(Throwable failureCause) {
        this.failureCause.set(failureCause);
    }

    Throwable getFailureCause() {
        return failureCause.get();
    }

    @Override
    public String toString() {
        return "JmsConnectionConsumer { id=" + consumerInfo.getId() + " }";
    }

    protected void checkClosed() throws IllegalStateException {
        if (closed.get()) {
            IllegalStateException jmsEx = null;

            if (getFailureCause() == null) {
                jmsEx = new IllegalStateException("The ConnectionConsumer is closed");
            } else {
                jmsEx = new IllegalStateException("The ConnectionConsumer was closed due to an unrecoverable error.");
                jmsEx.initCause(getFailureCause());
            }

            throw jmsEx;
        }
    }

    private boolean deliverNextPending() {
        if (messageQueue.isRunning() && !messageQueue.isEmpty()) {
            dispatchLock.lock();

            try {
                ServerSession serverSession = getServerSessionPool().getServerSession();
                if (serverSession == null) {
                    // There might not be an available session so queue a task to try again
                    // and hope that by then one is available in the pool.
                    dispatcher.schedule(new BoundedMessageDeliverTask(messageQueue.size()), DEFAULT_DISPATCH_RETRY_DELAY, TimeUnit.MILLISECONDS);
                    return false;
                }

                Session session = serverSession.getSession();

                JmsInboundMessageDispatch envelope = messageQueue.dequeueNoWait();

                if (session instanceof JmsSession) {
                    ((JmsSession) session).enqueueInSession(envelope);
                } else {
                    LOG.warn("ServerSession provided an onknown JMS Session type to this connection consumer: {}", session);
                }

                serverSession.start();
            } catch (JMSException e) {
                connection.onAsyncException(e);
                stop(true);
            } finally {
                dispatchLock.unlock();
            }
        }

        return !messageQueue.isEmpty();
    }

    private final class BoundedMessageDeliverTask implements Runnable {

        private final int deliveryCount;

        public BoundedMessageDeliverTask(int deliveryCount) {
            this.deliveryCount = deliveryCount;
        }

        @Override
        public void run() {
            int current = 0;

            while (messageQueue.isRunning() && current++ < deliveryCount) {
                if (!deliverNextPending()) {
                    return;  // Another task already drained the queue.
                }
            }
        }
    }
}