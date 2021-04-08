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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.NoOpAsyncResult;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.amqp.builders.AmqpEndpointBuilder;
import org.apache.qpid.jms.provider.exceptions.ProviderInvalidDestinationException;
import org.apache.qpid.protonj2.engine.Receiver;
import org.apache.qpid.protonj2.engine.Session;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.messaging.Target;
import org.apache.qpid.protonj2.types.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.types.transport.SenderSettleMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Subclass of the standard session object used solely by AmqpConnection to
 * aid in managing connection resources that require a persistent session.
 */
public class AmqpConnectionSession extends AmqpSession {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConnectionSession.class);

    private final Map<String, AsyncResult> pendingUnsubs = new HashMap<String, AsyncResult>();

    /**
     * Create a new instance of a Connection owned Session object.
     *
     * @param connection
     *        the connection that owns this session.
     * @param info
     *        the <code>JmsSessionInfo</code> for the Session to create.
     * @param session
     *        the Proton session instance that this resource wraps.
     */
    public AmqpConnectionSession(AmqpConnection connection, JmsSessionInfo info, Session session) {
        super(connection, info, session);
    }

    /**
     * Used to remove an existing durable topic subscription from the remote broker.
     *
     * @param subscriptionName
     *        the subscription name that is to be removed.
     * @param hasClientID
     *        whether the connection has a clientID set.
     * @param request
     *        the request that awaits the completion of this action.
     */
    public void unsubscribe(String subscriptionName, boolean hasClientID, AsyncResult request) {
        AmqpSubscriptionTracker subTracker = getConnection().getSubTracker();
        String linkName = subTracker.getFirstDurableSubscriptionLinkName(subscriptionName, hasClientID);

        DurableSubscriptionReattachBuilder builder =
            new DurableSubscriptionReattachBuilder(this, getResourceInfo(), linkName, subscriptionName);

        // Store in case the session is closed before this resource creation request is
        // answered by the remote.
        pendingUnsubs.put(subscriptionName, builder);

        LOG.debug("Attempting remove of subscription: {}", subscriptionName);
        builder.buildEndpoint(request);
    }

    @Override
    public void processEndpointClosed() {
        ProviderException cause = getFailureCause();
        if (cause == null) {
            cause = createEndpointRemotelyClosedException();
        }

        List<AsyncResult> pending = new ArrayList<>(pendingUnsubs.values());
        for (AsyncResult unsubscribeRequest : pending) {
            unsubscribeRequest.onFailure(cause);
        }

        super.processEndpointClosed();
    }

    private static final class DurableSubscriptionReattach extends AmqpAbstractEndpoint<JmsSessionInfo, Receiver> {

        public DurableSubscriptionReattach(AmqpSession session, JmsSessionInfo resource, Receiver receiver) {
            super(session.getProvider(), resource, receiver);
        }

        @Override
        public void processEndpointClosed() {
            // For unsubscribe we care if the remote signaled an error on the close since
            // that would indicate that the unsubscribe did not succeed and we want to throw
            // that from the unsubscribe call.
            if (hasRemoteError() && isAwaitingRemoteClose()) {
                setFailureCause(createEndpointRemotelyClosedException());
            }
        }
    }

    private final class DurableSubscriptionReattachBuilder extends AmqpEndpointBuilder<DurableSubscriptionReattach, AmqpSession, JmsSessionInfo, Receiver> implements AsyncResult {

        private final String linkName;
        private final String subscriptionName;
        private final boolean hasClientID;

        private DurableSubscriptionReattach subscription;
        private AsyncResult originalRequest;

        public DurableSubscriptionReattachBuilder(AmqpSession session, JmsSessionInfo resourceInfo, String linkName, String subscriptionName) {
            super(session.getProvider(), session, resourceInfo);

            this.hasClientID = parent.getConnection().getResourceInfo().isExplicitClientID();
            this.linkName = linkName;
            this.subscriptionName = subscriptionName;
        }

        @Override
        public void buildEndpoint(final AsyncResult request, Consumer<DurableSubscriptionReattach> resourceConsumer) {
            this.originalRequest = request;

            super.buildEndpoint(this, subscription -> {
                this.subscription = subscription;

                if (resourceConsumer != null) {
                    resourceConsumer.accept(subscription);
                }
            });
        }

        @Override
        protected Receiver createEndpoint(JmsSessionInfo resourceInfo) {
            Receiver receiver = getParent().getEndpoint().receiver(linkName);
            receiver.setTarget(new Target());
            receiver.setSenderSettleMode(SenderSettleMode.UNSETTLED);
            receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);

            if (!hasClientID) {
              // We are trying to unsubscribe a 'global' shared subs using a 'null source lookup', add link
              // desired capabilities as hints to the peer to consider this when trying to attach the link.
              receiver.setDesiredCapabilities(new Symbol[] { AmqpSupport.SHARED, AmqpSupport.GLOBAL });
            }

            return receiver;
        }

        @Override
        protected DurableSubscriptionReattach createResource(AmqpSession session, JmsSessionInfo resourceInfo, Receiver receiver) {
            return new DurableSubscriptionReattach(session, resourceInfo, receiver);
        }

        @Override
        protected boolean isClosePending() {
            // When no link terminus was created, the peer will now detach/close us otherwise
            // we need to validate the returned remote source prior to open completion.
            return endpoint.getRemoteSource() == null;
        }

        @Override
        public void onFailure(ProviderException result) {
            LOG.trace("Failed to reattach to subscription '{}' using link name '{}'", subscriptionName, linkName);
            pendingUnsubs.remove(subscriptionName);
            originalRequest.onFailure(result);
        }

        @Override
        public void onSuccess() {
            LOG.trace("Reattached to subscription '{}' using link name '{}'", subscriptionName, linkName);
            pendingUnsubs.remove(subscriptionName);
            if (subscription.getEndpoint().getRemoteSource() != null) {
                subscription.close(originalRequest);
            } else {
                subscription.close(NoOpAsyncResult.INSTANCE);
                originalRequest.onFailure(
                    new ProviderInvalidDestinationException("Cannot remove a subscription that does not exist"));
            }
        }

        @Override
        public boolean isComplete() {
            return originalRequest != null && originalRequest.isComplete();
        }
    }
}
