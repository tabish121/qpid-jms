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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JMS Connection Consumer implementation.
 */
public class JmsConnectionConsumer implements ConnectionConsumer, JmsMessageDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(JmsConnectionConsumer.class);

    private JmsConnection connection;
    private JmsConsumerInfo consumerInfo;
    private ServerSessionPool sessionPool;

    private final Lock dispatchLock = new ReentrantLock();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicReference<Throwable> failureCause = new AtomicReference<>();

    public JmsConnectionConsumer(JmsConnection connection, JmsConsumerInfo consumerInfo) throws JMSException {
        this.connection = connection;
        this.consumerInfo = consumerInfo;

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

        dispatchLock.lock();
        try {
            ServerSession serverSession = getServerSessionPool().getServerSession();
            Session session = serverSession.getSession();

            if (session instanceof JmsSession) {
                ((JmsSession) session).enqueueInSession(envelope);
            } else {
                LOG.warn("ServerSession provided an onknown JMS Session type to this connection consumer: {}", session);
            }

            serverSession.start();
        } catch (JMSException e) {
            connection.onAsyncException(e);
        } finally {
            dispatchLock.unlock();
        }
    }

    @Override
    public void close() throws JMSException {
        if (!closed.get()) {
            shutdown(null);
        }
    }

    protected void shutdown(Throwable cause) throws JMSException {
        if (closed.compareAndSet(false, true)) {
            dispatchLock.lock();
            try {
                failureCause.set(cause);
                consumerInfo.setState(ResourceState.CLOSED);
                connection.removeConnectionConsumer(consumerInfo);
                connection.destroyResource(consumerInfo);
            } finally {
                dispatchLock.unlock();
            }
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
}
