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

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.MODIFIED_FAILED;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.MODIFIED_FAILED_UNDELIVERABLE;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.REJECTED;

import java.util.ArrayList;
import java.util.ListIterator;
import java.util.concurrent.ScheduledFuture;

import javax.jms.Session;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsConsumerInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.ProviderListener;
import org.apache.qpid.jms.provider.WrappedAsyncResult;
import org.apache.qpid.jms.provider.amqp.message.AmqpCodec;
import org.apache.qpid.jms.provider.exceptions.ProviderExceptionSupport;
import org.apache.qpid.jms.provider.exceptions.ProviderOperationTimedOutException;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.engine.Receiver;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.apache.qpid.protonj2.types.transport.DeliveryState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQP Consumer object that is used to manage JMS MessageConsumer semantics.
 */
public class AmqpConsumer extends AmqpAbstractEndpoint<JmsConsumerInfo, Receiver> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConsumer.class);

    private static final int INDIVIDUAL_ACKNOWLEDGE = 101;

    protected final AmqpSession session;
    protected final int acknowledgementMode;
    protected AsyncResult stopRequest;
    protected AsyncResult pullRequest;
    protected long incomingSequence;
    protected int deliveredCount;
    protected int dispatchedCount;
    protected boolean deferredClose;

    public AmqpConsumer(AmqpSession session, JmsConsumerInfo info, Receiver receiver) {
        super(session.getProvider(), info, receiver);

        this.session = session;
        this.acknowledgementMode = info.getAcknowledgementMode();

        receiver.creditStateUpdateHandler(this::handleCreditStateUpdate)
                .deliveryStateUpdatedHandler(this::handleDeliveryReadOrUpdated)
                .deliveryReadHandler(this::handleDeliveryReadOrUpdated);
    }

    @Override
    public void close(AsyncResult request) {
        acknowledgeUndeliveredRecoveredMessages();

        // If we have pending deliveries we remain open to allow for ACK or for a
        // pending transaction that this consumer is active in to complete.
        if (shouldDeferClose()) {
            LOG.info("Consumer {} deferring close until delivered work is completed", getConsumerId());
            deferredClose = true;
            stop(new StopAndReleaseRequest(request));
        } else {
            LOG.info("Consumer {} closing immediately", getConsumerId());
            super.close(request);
        }
    }

    private void acknowledgeUndeliveredRecoveredMessages() {
        if (acknowledgementMode == Session.CLIENT_ACKNOWLEDGE
                || acknowledgementMode == Session.AUTO_ACKNOWLEDGE
                    || acknowledgementMode == Session.DUPS_OK_ACKNOWLEDGE
                        || acknowledgementMode == INDIVIDUAL_ACKNOWLEDGE) {
            // Send dispositions for any messages which were previously delivered and
            // session recovered, but were then not delivered again afterwards.
            getEndpoint().unsettled().forEach(delivery -> {
                JmsInboundMessageDispatch envelope = delivery.getLinkedResource(JmsInboundMessageDispatch.class);
                if (envelope.isRecovered() && !envelope.isDelivered()) {
                    handleDisposition(envelope, delivery, MODIFIED_FAILED);
                }
            });
        }
    }

    /**
     * Starts the consumer by setting the link credit to the given prefetch value.
     *
     * @param request
     *      The request that awaits completion of the consumer start.
     */
    public void start(AsyncResult request) {
        JmsConsumerInfo consumerInfo = getResourceInfo();
        if (consumerInfo.isListener() && consumerInfo.getPrefetchSize() == 0) {
            sendFlowForNoPrefetchListener();
        } else {
            sendFlowIfNeeded();
        }
        request.onSuccess();
    }

    /**
     * Stops the consumer, using all link credit and waiting for in-flight messages to arrive.
     *
     * @param request
     *      The request that awaits completion of the consumer stop.
     */
    public void stop(AsyncResult request) {
        Receiver receiver = getEndpoint();
        if (receiver.getCredit() > 0) {
            // TODO: We don't actually want the additional messages that could be sent while
            // draining. We could explicitly reduce credit first, or possibly use 'echo' instead
            // of drain if it was supported. We would first need to understand what happens
            // if we reduce credit below the number of messages already in-flight before
            // the peer sees the update.
            stopRequest = request;

            if (!receiver.isDraining()) {
                receiver.drain();
            }

            if (getDrainTimeout() > 0) {
                // If the remote doesn't respond we will close the consumer and break any
                // blocked receive or stop calls that are waiting, unless the consumer is
                // a participant in a transaction in which case we will just fail the request
                // and leave the consumer open since the TX needs it to remain active.
                final ScheduledFuture<?> future = getSession().schedule(() -> {
                    LOG.trace("Consumer {} drain request timed out", getConsumerId());
                    ProviderException cause = new ProviderOperationTimedOutException("Remote did not respond to a drain request in time");
                    if (session.isTransacted() && session.getTransactionContext().isInTransaction(getConsumerId())) {
                        stopRequest.onFailure(cause);
                        stopRequest = null;
                    } else {
                        close(cause);
                    }
                }, getDrainTimeout());

                stopRequest = new ScheduledRequest(future, stopRequest);
            }
        }
    }

    private void stopOnSchedule(long timeout, final AsyncResult request) {
        LOG.trace("Consumer {} scheduling stop", getConsumerId());
        // We need to drain the credit if no message(s) arrive to use it.
        final ScheduledFuture<?> future = getSession().schedule(() -> {
            LOG.trace("Consumer {} running scheduled stop", getConsumerId());
            stop(request);
        }, timeout);

        stopRequest = new ScheduledRequest(future, request);
    }

    /**
     * Called to acknowledge all messages that have been marked as delivered but
     * have not yet been marked consumed.  Usually this is called as part of an
     * client acknowledge session operation.
     *
     * Only messages that have already been acknowledged as delivered by the JMS
     * framework will be in the delivered Map.  This means that the link credit
     * would already have been given for these so we just need to settle them.
     *
     * @param ackType the type of acknowledgement to perform
     */
    public void acknowledge(ACK_TYPE ackType) {
        LOG.trace("Session Acknowledge for consumer {} with ack type {}", getResourceInfo().getId(), ackType);

        getEndpoint().unsettled().forEach(delivery -> {
            JmsInboundMessageDispatch envelope = delivery.getLinkedResource(JmsInboundMessageDispatch.class);
            if (ackType == ACK_TYPE.SESSION_SHUTDOWN && (envelope.isDelivered() || envelope.isRecovered())) {
                handleDisposition(envelope, delivery, MODIFIED_FAILED);
            } else if (envelope.isDelivered()) {
                final DeliveryState disposition;

                switch (ackType) {
                    case ACCEPTED:
                        disposition = Accepted.getInstance();
                        break;
                    case RELEASED:
                        disposition = Released.getInstance();
                        break;
                    case REJECTED:
                        disposition = REJECTED;
                        break;
                    case MODIFIED_FAILED:
                        disposition = MODIFIED_FAILED;
                        break;
                    case MODIFIED_FAILED_UNDELIVERABLE:
                        disposition = MODIFIED_FAILED_UNDELIVERABLE;
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid acknowledgement type specified: " + ackType);
                }

                handleDisposition(envelope, delivery, disposition);
            }
        });

        tryCompleteDeferredClose();
    }

    /**
     * Called to acknowledge a given delivery.
     *
     * @param envelope
     *        the delivery that is to be acknowledged.
     * @param ackType
     *        the type of acknowledgement to perform.
     */
    public void acknowledge(JmsInboundMessageDispatch envelope, ACK_TYPE ackType) {
        final IncomingDelivery delivery = envelope.getProviderHint(IncomingDelivery.class);

        if (delivery != null) {
            switch (ackType) {
                case DELIVERED:
                    handleDelivered(envelope, delivery);
                    break;
                case ACCEPTED:
                    handleAccepted(envelope, delivery);
                    break;
                case REJECTED:
                    handleDisposition(envelope, delivery, REJECTED);
                    break;
                case RELEASED:
                    handleDisposition(envelope, delivery, Released.getInstance());
                    break;
                case MODIFIED_FAILED:
                    handleDisposition(envelope, delivery, MODIFIED_FAILED);
                    break;
                case MODIFIED_FAILED_UNDELIVERABLE:
                    handleDisposition(envelope, delivery, MODIFIED_FAILED_UNDELIVERABLE);
                    break;
                default:
                    LOG.warn("Unsupported Ack Type for message: {}", envelope);
                    throw new IllegalArgumentException("Unknown Acknowledgement type");
            }

            sendFlowIfNeeded();
            tryCompleteDeferredClose();
        } else {
            LOG.warn("Received Ack for unknown message: {}", envelope);
        }
    }

    private void handleDelivered(JmsInboundMessageDispatch envelope, IncomingDelivery delivery) {
        LOG.debug("Delivered Ack of message: {}", envelope);
        deliveredCount++;
        envelope.setRecovered(false);
        envelope.setDelivered(true);
        delivery.setDefaultDeliveryState(MODIFIED_FAILED);
    }

    private void handleAccepted(JmsInboundMessageDispatch envelope, IncomingDelivery delivery) {
        LOG.debug("Accepted Ack of message: {}", envelope);
        if (!delivery.isRemotelySettled()) {
            if (session.isTransacted() && !getResourceInfo().isBrowser()) {
                if (session.isTransactionInDoubt()) {
                    LOG.trace("Skipping ack of message {} in failed transaction.", envelope);
                    return;
                }

                Binary txnId = session.getTransactionContext().getAmqpTransactionId();
                if (txnId != null) {
                    delivery.disposition(session.getTransactionContext().getTxnAcceptState(), true);
                    session.getTransactionContext().registerTxConsumer(this);
                }
            } else {
                delivery.disposition(Accepted.getInstance(), true);
            }
        } else {
            delivery.settle();
        }

        if (envelope.isDelivered()) {
            deliveredCount--;
        }
        dispatchedCount--;
    }

    private void handleDisposition(JmsInboundMessageDispatch envelope, IncomingDelivery delivery, DeliveryState outcome) {
        delivery.disposition(outcome, true);
        if (envelope.isDelivered()) {
            deliveredCount--;
        }
        dispatchedCount--;
    }

    /**
     * We only send more credits as the credit window dwindles to a certain point and
     * then we open the window back up to full prefetch size.  If this is a pull consumer
     * or we are stopping then we never send credit here.
     */
    private void sendFlowIfNeeded() {
        int prefetchSize = getResourceInfo().getPrefetchSize();
        if (prefetchSize == 0 || isStopping() || deferredClose) {
            // TODO: isStopping isn't effective when this method is called following
            // processing the last of any messages received while stopping, since that
            // happens just after we stopped. That may be ok in some situations however, and
            // if will only happen if prefetchSize != 0.
            return;
        }

        int currentCredit = getEndpoint().getCredit();
        if (currentCredit <= prefetchSize * 0.5) {
            int potentialPrefetch = currentCredit + (dispatchedCount - deliveredCount);

            if (potentialPrefetch <= prefetchSize * 0.7) {
                int additionalCredit = prefetchSize - potentialPrefetch;

                LOG.trace("Consumer {} granting additional credit: {}", getConsumerId(), additionalCredit);
                getEndpoint().addCredit(additionalCredit);
            }
        }
    }

    private void sendFlowForNoPrefetchListener() {
        int currentCredit = getEndpoint().getCredit();
        if (currentCredit < 1) {
            int additionalCredit = 1 - currentCredit;
            LOG.trace("Consumer {} granting additional credit: {}", getConsumerId(), additionalCredit);
            getEndpoint().addCredit(additionalCredit);
        }
    }

    /**
     * Recovers all previously delivered but not acknowledged messages.
     *
     * @throws Exception if an error occurs while performing the recover.
     */
    public void recover() throws Exception {
        LOG.debug("Session Recover for consumer: {}", getResourceInfo().getId());

        ArrayList<JmsInboundMessageDispatch> redispatchList = new ArrayList<JmsInboundMessageDispatch>();

        getEndpoint().unsettled().forEach((delivery) -> {
            JmsInboundMessageDispatch envelope = delivery.getLinkedResource(JmsInboundMessageDispatch.class);
            if (envelope.isDelivered()) {
                envelope.getMessage().getFacade().setRedeliveryCount(
                    envelope.getMessage().getFacade().getRedeliveryCount() + 1);
                envelope.setEnqueueFirst(true);
                envelope.setDelivered(false);
                envelope.setRecovered(true);

                redispatchList.add(envelope);
            }
        });

        // Previously delivered messages should be tagged as dispatched messages again so we
        // can properly compute the next credit refresh, so subtract them from both the delivered
        // and dispatched counts and then dispatch them again as a new message.
        deliveredCount -= redispatchList.size();
        dispatchedCount -= redispatchList.size();

        ListIterator<JmsInboundMessageDispatch> reverseIterator = redispatchList.listIterator(redispatchList.size());
        while (reverseIterator.hasPrevious()) {
            deliver(reverseIterator.previous());
        }

        if (deferredClose) {
            acknowledgeUndeliveredRecoveredMessages();
            tryCompleteDeferredClose();
        }
    }

    /**
     * Request a remote peer send a Message to this client.
     *
     *   {@literal timeout < 0} then it should remain open until a message is received.
     *   {@literal timeout = 0} then it returns a message or null if none available
     *   {@literal timeout > 0} then it should remain open for timeout amount of time.
     *
     * The timeout value when positive is given in milliseconds.
     *
     * @param timeout
     *        the amount of time to tell the remote peer to keep this pull request valid.
     * @param request
     *        the asynchronous request object waiting to be notified of the pull having completed.
     */
    public void pull(final long timeout, final AsyncResult request) {
        LOG.trace("Pull on consumer {} with timeout = {}", getConsumerId(), timeout);
        if (timeout < 0) {
            // Wait until message arrives. Just give credit if needed.
            if (getEndpoint().getCredit() == 0) {
                LOG.trace("Consumer {} granting 1 additional credit for pull.", getConsumerId());
                getEndpoint().addCredit(1);
            }

            // Await the message arrival
            pullRequest = request;
        } else if (timeout == 0) {
            // If we have no credit then we need to issue some so that we can
            // try to fulfill the request, then drain down what is there to
            // ensure we consume what is available and remove all credit.
            if (getEndpoint().getCredit() == 0) {
                LOG.trace("Consumer {} granting 1 additional credit for pull.", getConsumerId());
                getEndpoint().drain(1);
            }

            // Drain immediately and wait for the message(s) to arrive,
            // or a flow indicating removal of the remaining credit.
            stop(request);
        } else if (timeout > 0) {
            // If we have no credit then we need to issue some so that we can
            // try to fulfill the request, then drain down what is there to
            // ensure we consume what is available and remove all credit.
            if (getEndpoint().getCredit() == 0) {
                LOG.trace("Consumer {} granting 1 additional credit for pull.", getConsumerId());
                getEndpoint().addCredit(1);
            }

            // Wait for the timeout for the message(s) to arrive, then drain if required
            // and wait for remaining message(s) to arrive or a flow indicating
            // removal of the remaining credit.
            stopOnSchedule(timeout, request);
        }
    }

    public void processDeliveryUpdates(AmqpProvider provider, IncomingDelivery delivery) throws ProviderException {
        if (delivery.getDefaultDeliveryState() == null){
            delivery.setDefaultDeliveryState(Released.getInstance());
        }

        if (!delivery.isPartial()) {
            LOG.trace("{} has incoming Message(s).", this);
            try {
                if (processDelivery(delivery)) {
                    // We processed a message, signal completion
                    // of a message pull request if there is one.
                    if (pullRequest != null) {
                        pullRequest.onSuccess();
                        pullRequest = null;
                    }
                }
            } catch (Exception e) {
                throw ProviderExceptionSupport.createNonFatalOrPassthrough(e);
            }
        }

        // We have exhausted the locally queued messages on this link.
        // Check if we tried to stop and have now run out of credit.
        if (getEndpoint().getCredit() <= 0) {
            if (stopRequest != null) {
                stopRequest.onSuccess();
                stopRequest = null;
            }
        }
    }

    private boolean processDelivery(IncomingDelivery incoming) throws Exception {
        JmsMessage message = null;
        try {
            message = AmqpCodec.decodeMessage(this, incoming.readAll()).asJmsMessage();
        } catch (Exception e) {
            LOG.warn("Error on transform: {}", e.getMessage());
            LOG.trace("Error from transform of message: ", e);
            // TODO - We could signal provider error but not sure we want to fail
            //        the connection just because we can't convert the message.
            //        In the future once the JMS mapping is complete we should be
            //        able to convert everything to some message even if its just
            //        a bytes messages as a fall back.
            incoming.disposition(MODIFIED_FAILED_UNDELIVERABLE, true);
            // TODO: this flows credit, which we might not want, e.g if
            // a drain was issued to stop the link.
            sendFlowIfNeeded();
            return false;
        }

        // Let the message do any final processing before sending it onto a consumer.
        // We could defer this to a later stage such as the JmsConnection or even in
        // the JmsMessageConsumer dispatch method if we needed to.
        message.onDispatch();

        JmsInboundMessageDispatch envelope = new JmsInboundMessageDispatch(getNextIncomingSequenceNumber());
        envelope.setMessage(message);
        envelope.setConsumerId(getResourceInfo().getId());
        envelope.setConsumerInfo(getResourceInfo());
        // Store link to delivery in the hint for use in acknowledge requests.
        envelope.setProviderHint(incoming);
        envelope.setMessageId(message.getFacade().getProviderMessageIdObject());

        // Store reference to envelope in delivery context for recovery
        incoming.setLinkedResource(envelope);

        deliver(envelope);

        return true;
    }

    private void handleCreditStateUpdate(Receiver receiver) {
        // Check if we tried to stop and have now run out of credit, and
        // processed all locally queued messages
        if (stopRequest != null) {
            if (receiver.getCredit() == 0) {
                stopRequest.onSuccess();
                stopRequest = null;
            }
        }

        if (pullRequest != null) {
            if (receiver.getCredit() == 0) {
                pullRequest.onSuccess();
                pullRequest = null;
            }
        }

        LOG.trace("Consumer {} drain state updated, remote credit = {}", getConsumerId(), receiver.getCredit());
    }

    private void handleDeliveryReadOrUpdated(IncomingDelivery delivery) {
        if (delivery.getDefaultDeliveryState() == null){
            delivery.setDefaultDeliveryState(Released.getInstance());
        }

        if (delivery.isAborted()) {
            delivery.settle();
            sendFlowIfNeeded();
        } else  if (!delivery.isPartial() && delivery.available() != 0) {
            LOG.trace("{} has incoming Message(s).", this);
            try {
                if (processDelivery(delivery)) {
                    // We processed a message, signal completion
                    // of a message pull request if there is one.
                    if (pullRequest != null) {
                        pullRequest.onSuccess();
                        pullRequest = null;
                    }
                }
            } catch (Exception e) {
                // TODO: Close the Endpoint for now need to figure out what else should happen
                close(ProviderExceptionSupport.createNonFatalOrPassthrough(e));
            }
        }

        // We have exhausted the locally queued messages on this link.
        // Check if we tried to stop and have now run out of credit.
        if (getEndpoint().getCredit() <= 0) {
            if (stopRequest != null) {
                stopRequest.onSuccess();
                stopRequest = null;
            }
        }
    }

    protected long getNextIncomingSequenceNumber() {
        return ++incomingSequence;
    }

    @Override
    protected void doCloseOfWrappedEndpoint() {
        if (getResourceInfo().isDurable()) {
            getEndpoint().detach();
        } else {
            getEndpoint().close();
        }
    }

    public AmqpConnection getConnection() {
        return session.getConnection();
    }

    public AmqpSession getSession() {
        return session;
    }

    public JmsConsumerId getConsumerId() {
        return this.getResourceInfo().getId();
    }

    public JmsDestination getDestination() {
        return this.getResourceInfo().getDestination();
    }

    public boolean isStopping() {
        return stopRequest != null;
    }

    public int getDrainTimeout() {
        return session.getProvider().getDrainTimeout();
    }

    @Override
    public String toString() {
        return "AmqpConsumer { " + getResourceInfo().getId() + " }";
    }

    protected void deliver(JmsInboundMessageDispatch envelope) throws Exception {
        if (!deferredClose) {
            ProviderListener listener = session.getProvider().getProviderListener();
            if (listener != null) {
                LOG.debug("Dispatching received message: {}", envelope);
                dispatchedCount++;
                listener.onInboundMessage(envelope);
            } else {
                LOG.error("Provider listener is not set, message will be dropped: {}", envelope);
            }
        }
    }

    public void preCommit() {
    }

    public void preRollback() {
    }

    public void postCommit() {
        tryCompleteDeferredClose();
    }

    public void postRollback() {
        releasePrefetch();
        tryCompleteDeferredClose();
    }

    @Override
    protected void processEndpointClosed() {
        AmqpConnection connection = session.getConnection();
        AmqpSubscriptionTracker subTracker = connection.getSubTracker();
        JmsConsumerInfo consumerInfo = getResourceInfo();

        subTracker.consumerRemoved(consumerInfo);

        // When closed we need to release any pending tasks to avoid blocking
        final ProviderException cause = getFailureCause();

        if (stopRequest != null) {
            if (cause == null) {
                stopRequest.onSuccess();
            } else {
                stopRequest.onFailure(cause);
            }
            stopRequest = null;
        }

        if (pullRequest != null) {
            if (cause == null) {
                pullRequest.onSuccess();
            } else {
                pullRequest.onFailure(cause);
            }
            pullRequest = null;
        }
    }

    private boolean shouldDeferClose() {
        if (getSession().isTransacted() && getSession().getTransactionContext().isInTransaction(getConsumerId())) {
            return true;
        }

        if (deliveredCount > 0) {
            return true;
        }

        return false;
    }

    private void tryCompleteDeferredClose() {
        if (deferredClose && deliveredCount == 0) {
            super.close(new DeferredCloseRequest());
        }
    }

    private void releasePrefetch() {
        getEndpoint().unsettled().forEach(delivery -> {
            JmsInboundMessageDispatch envelope = delivery.getLinkedResource(JmsInboundMessageDispatch.class);
            if (!envelope.isDelivered()) {
                handleDisposition(envelope, delivery, Released.getInstance());
            }
        });
    }

    //----- Inner class used to report on deferred close ---------------------//

    private final class StopAndReleaseRequest extends WrappedAsyncResult {

        public StopAndReleaseRequest(AsyncResult closeRequest) {
            super(closeRequest);
        }

        @Override
        public void onSuccess() {
            // Now that the link is drained we can release all the prefetched
            // messages so that the remote can send them elsewhere.
            releasePrefetch();
            super.onSuccess();
        }
    }

    //----- Inner class used to report on deferred close ---------------------//

    private final class DeferredCloseRequest implements AsyncResult {

        @Override
        public void onFailure(ProviderException result) {
            LOG.trace("Failed deferred close of consumer: {} - {}", getConsumerId(), result.getMessage());
            getProvider().fireNonFatalProviderException(ProviderExceptionSupport.createNonFatalOrPassthrough(result));
        }

        @Override
        public void onSuccess() {
            LOG.trace("Completed deferred close of consumer: {}", getConsumerId());
        }

        @Override
        public boolean isComplete() {
            return isClosed();
        }
    }

    //----- Inner class used in message pull operations ----------------------//

    private static final class ScheduledRequest implements AsyncResult {

        private final ScheduledFuture<?> sheduledTask;
        private final AsyncResult origRequest;

        public ScheduledRequest(ScheduledFuture<?> completionTask, AsyncResult origRequest) {
            this.sheduledTask = completionTask;
            this.origRequest = origRequest;
        }

        @Override
        public void onFailure(ProviderException cause) {
            sheduledTask.cancel(false);
            origRequest.onFailure(cause);
        }

        @Override
        public void onSuccess() {
            boolean cancelled = sheduledTask.cancel(false);
            if (cancelled) {
                // Signal completion. Otherwise wait for the scheduled task to do it.
                origRequest.onSuccess();
            }
        }

        @Override
        public boolean isComplete() {
            return origRequest.isComplete();
        }
    }
}
