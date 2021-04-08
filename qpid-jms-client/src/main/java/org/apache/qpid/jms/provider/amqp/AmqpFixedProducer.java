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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsProducerId;
import org.apache.qpid.jms.meta.JmsProducerInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.exceptions.ProviderDeliveryModifiedException;
import org.apache.qpid.jms.provider.exceptions.ProviderDeliveryReleasedException;
import org.apache.qpid.jms.provider.exceptions.ProviderExceptionSupport;
import org.apache.qpid.jms.provider.exceptions.ProviderIllegalStateException;
import org.apache.qpid.jms.provider.exceptions.ProviderSendTimedOutException;
import org.apache.qpid.jms.provider.exceptions.ProviderUnsupportedOperationException;
import org.apache.qpid.jms.tracing.JmsTracer;
import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;
import org.apache.qpid.protonj2.engine.Sender;
import org.apache.qpid.protonj2.engine.impl.ProtonDeliveryTagGenerator;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.transactions.TransactionalState;
import org.apache.qpid.protonj2.types.transport.DeliveryState;
import org.apache.qpid.protonj2.types.transport.DeliveryState.DeliveryStateType;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;
import org.apache.qpid.protonj2.types.transport.SenderSettleMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQP Producer object that is used to manage JMS MessageProducer semantics.
 *
 * This Producer is fixed to a given JmsDestination and can only produce messages to it.
 */
public class AmqpFixedProducer extends AmqpAbstractEndpoint<JmsProducerInfo, Sender> implements AmqpProducer {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpFixedProducer.class);

    private final Map<Object, InFlightSend> blocked = new LinkedHashMap<Object, InFlightSend>();

    private final AmqpSession session;
    private final AmqpConnection connection;
    private final JmsTracer tracer;

    private boolean delayedDeliverySupported;

    public AmqpFixedProducer(AmqpSession session, JmsProducerInfo info, Sender sender) {
        super(session.getProvider(), info, sender);

        this.session = session;
        this.connection = session.getConnection();
        this.tracer = connection.getResourceInfo().getTracer();

        delayedDeliverySupported = connection.getProperties().isDelayedDeliverySupported();

        // Use a tag generator that will reuse old tags.  Later we might make this configurable.
        if (sender.getSenderSettleMode() == SenderSettleMode.SETTLED) {
            sender.setDeliveryTagGenerator(ProtonDeliveryTagGenerator.BUILTIN.EMPTY.createGenerator());
        } else {
            sender.setDeliveryTagGenerator(ProtonDeliveryTagGenerator.BUILTIN.POOLED.createGenerator());
        }

        sender.creditStateUpdateHandler(this::handleLinkCreditUpdate)
              .deliveryStateUpdatedHandler(this::handleDeliveryUpdated);
    }

    @Override
    public void close(AsyncResult request) {
        // If any sends are held we need to wait for them to complete.
        if (!blocked.isEmpty() || getEndpoint().hasUnsettled()) {
            this.closeRequest = request;
            return;
        }

        super.close(request);
    }

    @Override
    public void send(JmsOutboundMessageDispatch envelope, AsyncResult request) throws ProviderException {
        if (!isClosed()) {
            final InFlightSend send = new InFlightSend(envelope, request);

            if (!delayedDeliverySupported && envelope.getMessage().getFacade().isDeliveryTimeTransmitted()) {
                // Don't allow sends with delay if the remote has not said it can handle them
                send.onFailure(new ProviderUnsupportedOperationException("Remote does not support delayed message delivery"));
            } else if (session.isTransactionInDoubt()) {
                // If the transaction has failed due to remote termination etc then we just indicate
                // the send has succeeded until the a new transaction is started.
                send.onSuccess();
            } else if (getEndpoint().getCredit() <= 0) {
                LOG.trace("Holding Message send until credit is available.");

                if (getSendTimeout() > JmsConnectionInfo.INFINITE) {
                    send.requestTimeout = getProvider().scheduleRequestTimeout(send, getSendTimeout(), send);
                }

                blocked.put(envelope.getMessageId(), send);
            } else {
                doSend(envelope, send);
            }
        } else {
            request.onFailure(new ProviderIllegalStateException("The MessageProducer is closed"));
        }
    }

    private void doSend(JmsOutboundMessageDispatch envelope, InFlightSend send) throws ProviderException {
        LOG.trace("Producer sending message: {}", envelope);

        boolean presettle = envelope.isPresettle() || isPresettle();
        OutgoingDelivery delivery = getEndpoint().next();

        // Link the two delivery containers for later reconciliation
        send.setDelivery(delivery);
        delivery.setLinkedResource(send);

        // For pre-settled messages we can just mark as successful and we are done, but
        // for any other message we still track it until the remote settles.  If the send
        // was tagged as asynchronous we must mark the original request as complete but
        // we still need to wait for the disposition before we can consider the send as
        // having been successful.
        if (presettle) {
            send.onSuccess();
            delivery.settle();
        } else {
            if (envelope.isSendAsync()) {
                send.getOriginalRequest().onSuccess();
            }

            if (getSendTimeout() != JmsConnectionInfo.INFINITE && send.requestTimeout == null) {
                send.requestTimeout = getProvider().scheduleRequestTimeout(send, getSendTimeout(), send);
            }
        }

        if (session.isTransacted()) {
            AmqpTransactionContext context = session.getTransactionContext();
            delivery.disposition(context.getTxnEnrolledState());
            context.registerTxProducer(this);
        }

        // All delivery updates are done before this point so that the Transfer holds all
        // state data and no additional Disposition needs to be emitted.
        delivery.writeBytes((ProtonBuffer) envelope.getPayload());
    }

    protected void handleLinkCreditUpdate(Sender sender) {
        if (!blocked.isEmpty() && getEndpoint().isSendable()) {
            Iterator<InFlightSend> blockedSends = blocked.values().iterator();
            while (getEndpoint().getCredit() > 0 && blockedSends.hasNext()) {
                LOG.trace("Dispatching previously held send");
                InFlightSend held = blockedSends.next();
                try {
                    // If the transaction has failed due to remote termination etc then we just indicate
                    // the send has succeeded until the a new transaction is started.
                    if (session.isTransacted() && session.isTransactionInDoubt()) {
                        held.onSuccess();
                        return;
                    }

                    doSend(held.getEnvelope(), held);
                } catch (ProviderException e) {
                    // TODO: Investigate how this is handled.
                    throw new UncheckedIOException(new IOException(e));
                } finally {
                    blockedSends.remove();
                }
            }
        }

        if (blocked.isEmpty() && getEndpoint().isDraining()) {
            getEndpoint().drained();
        }
    }

    protected void handleDeliveryUpdated(OutgoingDelivery delivery) {
        DeliveryState state = delivery.getRemoteState();
        if (state != null) {
            InFlightSend send = (InFlightSend) delivery.getLinkedResource();

            if (state.getType() == DeliveryStateType.Accepted) {
                LOG.trace("Outcome of delivery was accepted: {}", delivery);
                send.onSuccess();
            } else {
                applyDeliveryStateUpdate(send, delivery, state);
            }
        }
    }

    private void applyDeliveryStateUpdate(InFlightSend send, OutgoingDelivery delivery, DeliveryState state) {
        ProviderException deliveryError = null;
        if (state == null) {
            return;
        }

        switch (state.getType()) {
            case Transactional:
                LOG.trace("State of delivery is Transactional, retrieving outcome: {}", state);
                applyDeliveryStateUpdate(send, delivery, (DeliveryState) ((TransactionalState) state).getOutcome());
                break;
            case Accepted:
                LOG.trace("Outcome of delivery was accepted: {}", delivery);
                send.onSuccess();
                break;
            case Rejected:
                LOG.trace("Outcome of delivery was rejected: {}", delivery);
                ErrorCondition remoteError = ((Rejected) state).getError();
                if (remoteError == null) {
                    remoteError = getEndpoint().getRemoteCondition();
                }

                deliveryError = AmqpSupport.convertToNonFatalException(getProvider(), remoteError);
                break;
            case Released:
                LOG.trace("Outcome of delivery was released: {}", delivery);
                deliveryError = new ProviderDeliveryReleasedException("Delivery failed: released by receiver");
                break;
            case Modified:
                LOG.trace("Outcome of delivery was modified: {}", delivery);
                Modified modified = (Modified) state;
                deliveryError = new ProviderDeliveryModifiedException("Delivery failed: failure at remote", modified);
                break;
            default:
                LOG.warn("Message send updated with unsupported state: {}", state);
        }

        if (deliveryError != null) {
            send.onFailure(deliveryError);
        }
    }

    @Override
    public JmsProducerId getProducerId() {
        return getResourceInfo().getId();
    }

    public void setDelayedDeliverySupported(boolean delayedDeliverySupported) {
        this.delayedDeliverySupported = delayedDeliverySupported;
    }

    @Override
    public boolean isAnonymous() {
        return getResourceInfo().getDestination() == null;
    }

    @Override
    public boolean isPresettle() {
        return getEndpoint().getSenderSettleMode() == SenderSettleMode.SETTLED;
    }

    @Override
    public String toString() {
        return "AmqpFixedProducer { " + getProducerId() + " }";
    }

    @Override
    protected void processEndpointClosed() {
        ProviderException error = getFailureCause();
        if (error == null) {
            // Producer might be awaiting a close in which case we won't signal failure to the close
            // caller but we should use the most appropriate error for failing the sends so we need
            // to check here if there a remote error and use that or create a descriptive error.
            if (hasRemoteError()) {
                error = createEndpointRemotelyClosedException();
            } else {
                error = new ProviderException("Producer closed remotely before message transfer result was notified");
            }
        }

        for (OutgoingDelivery delivery : getEndpoint().unsettled()) {
            try {
                delivery.getLinkedResource(InFlightSend.class).onFailure(error);
            } catch (Exception e) {
                LOG.debug("Caught exception when failing pending send during remote producer closure: {}", delivery, e);
            }
        }

        Collection<InFlightSend> blockedSends = new ArrayList<InFlightSend>(blocked.values());
        for (InFlightSend send : blockedSends) {
            try {
                send.onFailure(error);
            } catch (Exception e) {
                LOG.debug("Caught exception when failing blocked send during remote producer closure: {}", send, e);
            }
        }
    }

    private long getSendTimeout() {
        return getProvider().getSendTimeout();
    }

    //----- Class used to manage held sends ----------------------------------//

    private final class InFlightSend implements AsyncResult, AmqpExceptionBuilder {

        private final JmsOutboundMessageDispatch envelope;
        private final AsyncResult request;

        private OutgoingDelivery delivery;
        private ScheduledFuture<?> requestTimeout;

        public InFlightSend(JmsOutboundMessageDispatch envelope, AsyncResult request) {
            this.envelope = envelope;
            this.request = request;
        }

        @Override
        public void onFailure(ProviderException cause) {
            handleSendCompletion(false);

            if (request.isComplete()) {
                // Asynchronous sends can still be awaiting a completion in which case we
                // send to them otherwise send to the listener to be reported.
                if (envelope.isCompletionRequired()) {
                    getProvider().getProviderListener().onFailedMessageSend(envelope, ProviderExceptionSupport.createNonFatalOrPassthrough(cause));
                } else {
                    getProvider().fireNonFatalProviderException(ProviderExceptionSupport.createNonFatalOrPassthrough(cause));
                }
            } else {
                request.onFailure(cause);
            }
        }

        @Override
        public void onSuccess() {
            handleSendCompletion(true);

            if (!request.isComplete()) {
                request.onSuccess();
            }

            if (envelope.isCompletionRequired()) {
                getProvider().getProviderListener().onCompletedMessageSend(envelope);
            }
        }

        public void setRequestTimeout(ScheduledFuture<?> requestTimeout) {
            if (this.requestTimeout != null) {
                this.requestTimeout.cancel(false);
            }

            this.requestTimeout = requestTimeout;
        }

        public JmsOutboundMessageDispatch getEnvelope() {
            return envelope;
        }

        public AsyncResult getOriginalRequest() {
            return request;
        }

        public void setDelivery(OutgoingDelivery delivery) {
            this.delivery = delivery;
        }

        public OutgoingDelivery getDelivery() {
            return delivery;
        }

        @Override
        public boolean isComplete() {
            return request.isComplete();
        }

        private void handleSendCompletion(boolean successful) {
            setRequestTimeout(null);

            final OutgoingDelivery delivery = getDelivery();

            // Null delivery means that we never had credit to send so no delivery was created to carry the message
            // but it would be cached in the blocked delivery map so we need to remove it now.
            if (delivery != null && delivery.getLink().isLocallyOpen()) {
                // TODO: This could throw since it can write a settlement so we need to handle this
                //       differently than we did when proton did temporal squashing of work.
                delivery.settle();
            } else {
                blocked.remove(envelope.getMessageId());
            }

            // Null delivery means that we never had credit to send so no delivery was created to carry the message.
            if (delivery != null) {
                DeliveryState remoteState = delivery.getRemoteState();
                tracer.completeSend(envelope.getMessage().getFacade(), remoteState == null ? null : remoteState.getType().name());
            } else {
                blocked.remove(envelope.getMessageId());
                tracer.completeSend(envelope.getMessage().getFacade(), null);
            }

            // Put the message back to usable state following send complete
            envelope.getMessage().onSendComplete();

            // Once the pending sends queue is drained and all in-flight sends have been
            // settled we can propagate the close request.
            if (isAwaitingRemoteClose() && !isClosed() && blocked.isEmpty() && !getEndpoint().hasUnsettled()) {
                AmqpFixedProducer.super.close(closeRequest);
            }
        }

        @Override
        public ProviderException createException() {
            if (delivery == null) {
                return new ProviderSendTimedOutException("Timed out waiting for credit to send Message", envelope.getMessage());
            } else {
                return new ProviderSendTimedOutException("Timed out waiting for disposition of sent Message", envelope.getMessage());
            }
        }
    }
}
