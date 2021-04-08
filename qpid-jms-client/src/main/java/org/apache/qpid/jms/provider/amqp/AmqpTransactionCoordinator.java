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

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ScheduledFuture;

import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.meta.JmsTransactionId;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.amqp.AmqpTransactionContext.DischargeCompletion;
import org.apache.qpid.jms.provider.amqp.message.AmqpCodec;
import org.apache.qpid.jms.provider.exceptions.ProviderIllegalStateException;
import org.apache.qpid.jms.provider.exceptions.ProviderOperationTimedOutException;
import org.apache.qpid.jms.provider.exceptions.ProviderTransactionInDoubtException;
import org.apache.qpid.jms.provider.exceptions.ProviderTransactionRolledBackException;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;
import org.apache.qpid.protonj2.engine.Sender;
import org.apache.qpid.protonj2.engine.impl.ProtonDeliveryTagGenerator;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Section;
import org.apache.qpid.protonj2.types.transactions.Declare;
import org.apache.qpid.protonj2.types.transactions.Declared;
import org.apache.qpid.protonj2.types.transactions.Discharge;
import org.apache.qpid.protonj2.types.transport.DeliveryState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the AMQP Transaction coordinator link used by the transaction context
 * of a session to control the lifetime of a given transaction.
 */
public class AmqpTransactionCoordinator extends AmqpAbstractEndpoint<JmsSessionInfo, Sender> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpTransactionCoordinator.class);

    private static final Boolean ROLLBACK_MARKER = Boolean.FALSE;
    private static final Boolean COMMIT_MARKER = Boolean.TRUE;

    private final Queue<OperationContext> blocked = new ArrayDeque<>(2);

    public AmqpTransactionCoordinator(AmqpTransactionContext context, JmsSessionInfo resourceInfo, Sender sender) {
        super(context.getProvider(), resourceInfo, sender);

        // Use a tag generator that will reuse old tags.  Later we might make this configurable.
        sender.setDeliveryTagGenerator(ProtonDeliveryTagGenerator.BUILTIN.POOLED.createGenerator());

        sender.creditStateUpdateHandler(this::handleLinkCreditUpdate)
              .deliveryStateUpdatedHandler(this::handleDeliveryUpdated);
    }

    protected void handleLinkCreditUpdate(Sender sender) {
        if (!blocked.isEmpty() && getEndpoint().isSendable()) {
            while (getEndpoint().getCredit() > 0 && !blocked.isEmpty()) {
                LOG.trace("Dispatching previously held TXN operation");
                OperationContext held = blocked.poll();
                dispatch(held);
            }
        }

        if (blocked.isEmpty() && getEndpoint().isDraining()) {
            getEndpoint().drained();
        }
    }

    protected void handleDeliveryUpdated(OutgoingDelivery delivery) {
        if (delivery != null && delivery.isRemotelySettled()) {
            DeliveryState state = delivery.getRemoteState();

            if (delivery.getLinkedResource(OperationContext.class) == null) {
                return;
            }

            OperationContext context = (OperationContext) delivery.getLinkedResource();

            AsyncResult pendingRequest = context.getRequest();
            JmsTransactionId txId = context.getTransactionId();

            if (state instanceof Declared) {
                LOG.debug("New TX started: {}", txId);
                Declared declared = (Declared) state;
                txId.setProviderHint(declared.getTxnId());
                pendingRequest.onSuccess();
            } else if (state instanceof Rejected) {
                LOG.debug("Last TX request failed: {}", txId);
                Rejected rejected = (Rejected) state;
                ProviderException cause = AmqpSupport.convertToNonFatalException(getProvider(), rejected.getError());
                if (COMMIT_MARKER.equals(txId.getProviderContext()) && !(cause instanceof ProviderTransactionRolledBackException)){
                    cause = new ProviderTransactionRolledBackException(cause.getMessage(), cause);
                } else {
                    cause = new ProviderTransactionInDoubtException(cause.getMessage(), cause);
                }

                txId.setProviderHint(null);
                pendingRequest.onFailure(cause);
            } else {
                LOG.debug("Last TX request succeeded: {}", txId);
                pendingRequest.onSuccess();
            }

            // Reset state for next TX action.
            delivery.settle();
            pendingRequest = null;

            if (context.getTimeout() != null) {
                context.getTimeout().cancel(false);
            }
        }
    }

    public void declare(JmsTransactionId txId, AsyncResult request) throws ProviderException {
        if (isClosed()) {
            request.onFailure(new ProviderIllegalStateException("Cannot start new transaction: Coordinator remotely closed"));
            return;
        }

        if (txId.getProviderHint() != null) {
            throw new ProviderIllegalStateException("Declar called while a TX is still Active.");
        }

        ScheduledFuture<?> timeout = scheduleTimeoutIfNeeded("Timed out waiting for declare of TX.", request);
        OperationContext context = new OperationContext(Operation.DECLARE, txId, request, timeout);

        if (getEndpoint().isSendable()) {
            dispatch(context);
        } else {
            blocked.offer(context);
        }
    }

    public void discharge(JmsTransactionId txId, DischargeCompletion request) throws ProviderException {
        if (isClosed()) {
            ProviderException failureCause = null;

            if (request.isCommit()) {
                failureCause = new ProviderTransactionRolledBackException("Transaction inbout: Coordinator remotely closed");
            } else {
                failureCause = new ProviderIllegalStateException("Rollback cannot complete: Coordinator remotely closed");
            }

            request.onFailure(failureCause);
            return;
        }

        if (txId.getProviderHint() == null) {
            throw new ProviderIllegalStateException("Discharge called with no active Transaction.");
        }

        // Store the context of this action in the transaction ID for later completion.
        txId.setProviderContext(request.isCommit() ? COMMIT_MARKER : ROLLBACK_MARKER);

        ScheduledFuture<?> timeout = scheduleTimeoutIfNeeded("Timed out waiting for discharge of TX.", request);
        OperationContext context = new OperationContext(Operation.DISCHARGE, txId, request, timeout);

        if (getEndpoint().isSendable()) {
            dispatch(context);
        } else {
            blocked.offer(context);
        }
    }

    private void dispatch(OperationContext context) {
        final OutgoingDelivery delivery = getEndpoint().next();
        final Section<?> body;

        if (context.operation == Operation.DECLARE) {
            body = new AmqpValue<Declare>(new Declare());
        } else {
            Discharge discharge = new Discharge();
            DischargeCompletion completion = (DischargeCompletion) context.getRequest();
            discharge.setFail(!completion.isCommit());
            discharge.setTxnId((Binary) context.getTransactionId().getProviderHint());

            body = new AmqpValue<Discharge>(discharge);
        }

        delivery.setLinkedResource(context);
        delivery.writeBytes(AmqpCodec.encode(body));
    }

    //----- Base class overrides ---------------------------------------------//

    @Override
    protected void processEndpointClosed() {
        // Alert any pending operation that the link failed to complete the pending
        // begin / commit / rollback operation.

        // Producer might be awaiting a close in which case we won't signal failure to the close
        // caller but we should use the most appropriate error for failing the sends so we need
        // to check here if there a remote error and use that or create a descriptive error.
        final ProviderException error;
        if (getFailureCause() != null) {
            error = getFailureCause();
        } else if (hasRemoteError()) {
            error = createEndpointRemotelyClosedException();
        } else {
            error = new ProviderException("TXN Coordinator closed remotely before operation outcome could be determined");
        }

        getEndpoint().unsettled().forEach(delivery -> {
            try {
                OperationContext context = delivery.getLinkedResource(OperationContext.class);
                context.request.onFailure(error);
            } catch (Exception ex) {
                LOG.debug("Caught unexpected error while cancelling inflight TX Operations", ex);
            }
        });

        for (OperationContext operation : blocked) {
            try {
                operation.getRequest().onFailure(error);
            } catch (Exception e) {
                LOG.debug("Caught exception when failing blocked TXN operation during remote closure: {}", operation, e);
            }
        }
    }

    @Override
    protected void signalEndpointRemotelyClosed() {
        // Override the base class version because we do not want to propagate
        // an error up to the client if remote close happens as that is an
        // acceptable way for the remote to indicate the discharge could not
        // be applied.

        LOG.debug("Transaction Coordinator link {} was remotely closed", getResourceInfo());
    }

    //----- Internal implementation ------------------------------------------//

    private enum Operation {
        DECLARE,
        DISCHARGE
    }

    private class OperationContext {

        private final Operation operation;
        private final AsyncResult request;
        private final ScheduledFuture<?> timeout;
        private final JmsTransactionId transactionId;

        public OperationContext(Operation operation, JmsTransactionId transactionId, AsyncResult request, ScheduledFuture<?> timeout) {
            this.operation = operation;
            this.transactionId = transactionId;
            this.request = request;
            this.timeout = timeout;
        }

        public JmsTransactionId getTransactionId() {
            return transactionId;
        }

        public AsyncResult getRequest() {
            return request;
        }

        public ScheduledFuture<?> getTimeout() {
            return timeout;
        }
    }

    private ScheduledFuture<?> scheduleTimeoutIfNeeded(String cause, AsyncResult pendingRequest) {
        AmqpProvider provider = getProvider();
        if (provider.getRequestTimeout() != JmsConnectionInfo.INFINITE) {
            return provider.scheduleRequestTimeout(pendingRequest, provider.getRequestTimeout(), new ProviderOperationTimedOutException(cause));
        } else {
            return null;
        }
    }
}
