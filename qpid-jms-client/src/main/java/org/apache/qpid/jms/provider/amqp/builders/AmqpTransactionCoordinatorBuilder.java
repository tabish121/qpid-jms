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
package org.apache.qpid.jms.provider.amqp.builders;

import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.provider.amqp.AmqpTransactionContext;
import org.apache.qpid.jms.provider.amqp.AmqpTransactionCoordinator;
import org.apache.qpid.protonj2.engine.Endpoint;
import org.apache.qpid.protonj2.engine.Sender;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.transactions.Coordinator;
import org.apache.qpid.protonj2.types.transactions.TxnCapability;
import org.apache.qpid.protonj2.types.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.types.transport.SenderSettleMode;

/**
 * AMQP Transaction Coordinator {@link Endpoint} builder.
 */
public class AmqpTransactionCoordinatorBuilder extends AmqpEndpointBuilder<AmqpTransactionCoordinator, AmqpTransactionContext, JmsSessionInfo, Sender> {

    public AmqpTransactionCoordinatorBuilder(AmqpTransactionContext context, JmsSessionInfo resourceInfo) {
        super(context.getProvider(), context, resourceInfo);
    }

    @Override
    protected Sender createEndpoint(JmsSessionInfo resourceInfo) {
        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);

        Symbol[] outcomes = new Symbol[]{ Accepted.DESCRIPTOR_SYMBOL,
                                          Rejected.DESCRIPTOR_SYMBOL,
                                          Released.DESCRIPTOR_SYMBOL,
                                          Modified.DESCRIPTOR_SYMBOL };

        Source source = new Source();
        source.setOutcomes(outcomes);

        String coordinatorName = "qpid-jms:coordinator:" + resourceInfo.getId().toString();

        Sender sender = getParent().getSession().getEndpoint().sender(coordinatorName);
        sender.setSource(source);
        sender.setTarget(coordinator);
        sender.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        sender.setReceiverSettleMode(ReceiverSettleMode.FIRST);

        return sender;
    }

    @Override
    protected AmqpTransactionCoordinator createResource(AmqpTransactionContext context, JmsSessionInfo resourceInfo, Sender sender) {
        return new AmqpTransactionCoordinator(context, resourceInfo, sender);
    }

    @Override
    protected boolean isClosePending() {
        // When no link terminus was created, the peer will now detach/close us otherwise
        // we need to validate the returned remote source prior to open completion.
        return getEndpoint().getRemoteTarget() == null;
    }
}
