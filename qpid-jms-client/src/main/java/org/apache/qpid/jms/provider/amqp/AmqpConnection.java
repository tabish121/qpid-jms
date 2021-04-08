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

import java.net.URI;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.jms.JmsTemporaryDestination;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsSessionId;
import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.amqp.builders.AmqpSessionBuilder;
import org.apache.qpid.jms.provider.amqp.builders.AmqpTemporaryDestinationBuilder;
import org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFactory;
import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.engine.Sender;
import org.apache.qpid.protonj2.engine.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpConnection extends AmqpAbstractEndpoint<JmsConnectionInfo, Connection> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConnection.class);

    private final AmqpSubscriptionTracker subTracker = new AmqpSubscriptionTracker();
    private final AmqpJmsMessageFactory amqpMessageFactory;

    private final URI remoteURI;
    private final AmqpConnectionProperties properties;
    private AmqpConnectionSession connectionSession;

    private boolean objectMessageUsesAmqpTypes = false;

    public AmqpConnection(AmqpProvider provider, AmqpConnectionProperties properties, JmsConnectionInfo info, Connection protonConnection) {
        super(provider, info, protonConnection);

        this.properties = properties;
        this.remoteURI = provider.getRemoteURI();
        this.amqpMessageFactory = new AmqpJmsMessageFactory(this);
    }

    public void createSession(JmsSessionInfo sessionInfo, AsyncResult request) {
        new AmqpSessionBuilder(this, sessionInfo).buildEndpoint(request);
    }

    public void createTemporaryDestination(JmsTemporaryDestination destination, AsyncResult request) {
        new AmqpTemporaryDestinationBuilder(connectionSession, destination).buildEndpoint(request);
    }

    public AmqpTemporaryDestination getTemporaryDestination(JmsTemporaryDestination destination) {
        for (Sender sender : connectionSession.getEndpoint().senders()) {
            try {
                AmqpTemporaryDestination temp = sender.getLinkedResource(AmqpTemporaryDestination.class);
                if (destination.equals(temp.getResourceInfo())) {
                    return temp;
                }
            } catch (ClassCastException ex) {
                // Not an AmqpTempDestination continue searching
            }
        }

        return null;
    }

    public void unsubscribe(String subscriptionName, AsyncResult request) {
        // Check if there is an active (i.e open subscriber) shared or exclusive durable subscription using this name
        if (subTracker.isActiveDurableSub(subscriptionName)) {
            request.onFailure(new ProviderException("Can't remove an active durable subscription: " + subscriptionName));
            return;
        }

        boolean hasClientID = getResourceInfo().isExplicitClientID();

        connectionSession.unsubscribe(subscriptionName, hasClientID, request);
    }

    @Override
    protected void processEndpointClosed() {
        getEndpoint().sessions().forEach(session -> {
            try {
                if (session.getLinkedResource() != null) {
                    session.getLinkedResource(AmqpSession.class).processParentEndpointClosed(this);
                }
            } catch (ClassCastException cse) {
                // Unknown Session in Connection is a bit odd but could be opened session that is
                // waiting for remote close response.
            }
        });
    }

    public URI getRemoteURI() {
        return remoteURI;
    }

    public String getQueuePrefix() {
        return properties.getQueuePrefix();
    }

    public void setQueuePrefix(String queuePrefix) {
        properties.setQueuePrefix(queuePrefix);
    }

    public String getTopicPrefix() {
        return properties.getTopicPrefix();
    }

    public void setTopicPrefix(String topicPrefix) {
        properties.setTopicPrefix(topicPrefix);
    }

    /**
     * Retrieve the indicated Session instance from the list of active sessions.
     *
     * @param sessionId
     *        The JmsSessionId that's associated with the target session.
     *
     * @return the AmqpSession associated with the given id.
     */
    public AmqpSession getSession(JmsSessionId sessionId) {
        if (sessionId.getProviderHint() instanceof AmqpSession) {
            return (AmqpSession) sessionId.getProviderHint();
        }

        for (Session session : getEndpoint().sessions()) {
            AmqpSession amqpSession = session.getLinkedResource(AmqpSession.class);
            if (amqpSession.getSessionId().equals(sessionId)) {
                return amqpSession;
            }
        }

        throw new IllegalArgumentException("Could not find matching AmqpSession for id: " + sessionId);
    }

    /**
     * Retrieves the AmqpConnectionSession owned by this AmqpConnection.
     *
     * @return the AmqpConnectionSession owned by this AmqpConnection.
     */
    public AmqpConnectionSession getConnectionSession() {
        return connectionSession;
    }

    /**
     * Sets the {@link AmqpConnectionSession} to use in this {@link AmqpConnection} instance.
     *
     * @param session
     * 		The new {@link AmqpConnectionSession} instance to use.
     */
    public void setConnectionSession(AmqpConnectionSession session) {
        this.connectionSession = session;
    }

    /**
     * @return true if new ObjectMessage instance should default to using AMQP Typed bodies.
     */
    public boolean isObjectMessageUsesAmqpTypes() {
        return objectMessageUsesAmqpTypes;
    }

    /**
     * Configures the body type used in ObjectMessage instances that are sent from
     * this connection.
     *
     * @param objectMessageUsesAmqpTypes
     *        the objectMessageUsesAmqpTypes value to set.
     */
    public void setObjectMessageUsesAmqpTypes(boolean objectMessageUsesAmqpTypes) {
        this.objectMessageUsesAmqpTypes = objectMessageUsesAmqpTypes;
    }

    /**
     * @return the configured max number of cached anonymous fallback producers to keep.
     */
    public int getAnonymousProducerCacheSize() {
        return getProvider().getAnonymousFallbackCacheSize();
    }

    /**
     * @return The configured time before a cache anonymous producer link is close due to inactivity.
     */
    public int getAnonymousProducerCacheTimeout() {
        return getProvider().getAnonymousFallbackCacheTimeout();
    }

    /**
     * @return the AMQP based JmsMessageFactory for this Connection.
     */
    public AmqpJmsMessageFactory getAmqpMessageFactory() {
        return amqpMessageFactory;
    }

    /**
     * Returns the connection properties for an established connection which defines the various
     * capabilities and configuration options of the remote connection.  Prior to the establishment
     * of a connection this method returns null.
     *
     * @return the properties available for this connection or null if not connected.
     */
    public AmqpConnectionProperties getProperties() {
        return properties;
    }

    public AmqpSubscriptionTracker getSubTracker() {
        return subTracker;
    }

    /**
     * Allows a connection resource to schedule a task for future execution.
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

    /**
     * Allows a connection resource to schedule a task for future execution which will start after the
     * given delay and then repeat with a fixed delay between the end of one execution of the task and
     * the beginning of the next execution.
     *
     * @param task
     *      The Runnable task to be executed after the given delay.
     * @param delay
     *      The delay in milliseconds to schedule the given task for execution.
     *
     * @return a ScheduledFuture instance that can be used to cancel the task.
     */
    public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable task, long delay) {
        if (task == null) {
            LOG.trace("Resource attempted to schedule a null task.");
            return null;
        }

        return getProvider().getScheduler().scheduleWithFixedDelay(task, delay, delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public String toString() {
        return "AmqpConnection { " + getResourceInfo().getId() + " }";
    }

    //----- Override remote close handling in base class for Connection specific error handling.

    @Override
    protected ProviderException createEndpointRemotelyClosedException() {
        return AmqpSupport.convertToConnectionClosedException(getProvider(), getEndpoint().getRemoteCondition());
    }

    @Override
    protected void signalEndpointRemotelyClosed() {
        getProvider().fireProviderException(createEndpointRemotelyClosedException());
    }
}
