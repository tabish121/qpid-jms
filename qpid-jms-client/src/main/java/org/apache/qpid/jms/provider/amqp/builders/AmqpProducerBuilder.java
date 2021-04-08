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

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.DELAYED_DELIVERY;

import java.util.Arrays;
import java.util.List;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.meta.JmsProducerInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.amqp.AmqpAnonymousFallbackProducer;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;
import org.apache.qpid.jms.provider.amqp.AmqpFixedProducer;
import org.apache.qpid.jms.provider.amqp.AmqpProducer;
import org.apache.qpid.jms.provider.amqp.AmqpSession;
import org.apache.qpid.jms.provider.amqp.AmqpSupport;
import org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper;
import org.apache.qpid.jms.provider.exceptions.ProviderInvalidDestinationException;
import org.apache.qpid.protonj2.engine.Sender;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.messaging.Target;
import org.apache.qpid.protonj2.types.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.types.transport.SenderSettleMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQP {@link Sender} link builder that creates {@link AmqpProducer} wrappers around the
 * opened {@link Sender} links with the given configuration.
 */
public class AmqpProducerBuilder extends AmqpEndpointBuilder<AmqpProducer, AmqpSession, JmsProducerInfo, Sender> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpProducerBuilder.class);

    private boolean delayedDeliverySupported;

    public AmqpProducerBuilder(AmqpSession session, JmsProducerInfo resourceInfo) {
        super(session.getProvider(), session, resourceInfo);
    }

    @Override
    public void buildEndpoint(final AsyncResult request) {
        if (getResourceInfo().getDestination() == null && !getParent().getConnection().getProperties().isAnonymousRelaySupported()) {
            LOG.debug("Creating an AmqpAnonymousFallbackProducer");
            new AmqpAnonymousFallbackProducer(getParent(), getResourceInfo());
            request.onSuccess();
        } else {
            LOG.debug("Creating AmqpFixedProducer for: {}", getResourceInfo().getDestination());
            super.buildEndpoint(request);
        }
    }

    @Override
    protected Sender createEndpoint(JmsProducerInfo resourceInfo) {
        JmsDestination destination = resourceInfo.getDestination();
        AmqpConnection connection = getParent().getConnection();

        String targetAddress = AmqpDestinationHelper.getDestinationAddress(destination, connection);

        Symbol[] outcomes = new Symbol[]{ Accepted.DESCRIPTOR_SYMBOL, Rejected.DESCRIPTOR_SYMBOL, Released.DESCRIPTOR_SYMBOL, Modified.DESCRIPTOR_SYMBOL };
        String sourceAddress = resourceInfo.getId().toString();
        Source source = new Source();
        source.setAddress(sourceAddress);
        source.setOutcomes(outcomes);
        // TODO: default outcome. Accepted normally, Rejected for transaction controller?

        Target target = new Target();
        target.setAddress(targetAddress);
        Symbol typeCapability =  AmqpDestinationHelper.toTypeCapability(destination);
        if (typeCapability != null) {
            target.setCapabilities(typeCapability);
        }

        String senderName = "qpid-jms:sender:" + sourceAddress + ":" + targetAddress;

        Sender sender = getParent().getEndpoint().sender(senderName);
        sender.setSource(source);
        sender.setTarget(target);
        if (resourceInfo.isPresettle()) {
            sender.setSenderSettleMode(SenderSettleMode.SETTLED);
        } else {
            sender.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        }
        sender.setReceiverSettleMode(ReceiverSettleMode.FIRST);

        if (!connection.getProperties().isDelayedDeliverySupported()) {
            sender.setDesiredCapabilities(new Symbol[] { AmqpSupport.DELAYED_DELIVERY });
        } else {
            delayedDeliverySupported = true;
        }

        return sender;
    }

    @Override
    protected AmqpProducer createResource(AmqpSession parent, JmsProducerInfo resourceInfo, Sender endpoint) {
        AmqpFixedProducer producer = new AmqpFixedProducer(getParent(), getResourceInfo(), endpoint);

        producer.setDelayedDeliverySupported(delayedDeliverySupported);

        return producer;
    }

    @Override
    protected void processEndpointRemotelyOpened(Sender sender, JmsProducerInfo resourceInfo) {
        if (!delayedDeliverySupported) {
            Symbol[] remoteOfferedCapabilities = endpoint.getRemoteOfferedCapabilities();

            boolean supported = false;
            if (remoteOfferedCapabilities != null) {
                List<Symbol> list = Arrays.asList(remoteOfferedCapabilities);
                if (list.contains(DELAYED_DELIVERY)) {
                    supported = true;
                }
            }

            delayedDeliverySupported = supported;
        }
    }

    @Override
    protected boolean isClosePending() {
        // When no link terminus was created, the peer will now detach/close us otherwise
        // we need to validate the returned remote source prior to open completion.
        return getEndpoint().getRemoteTarget() == null;
    }

    @Override
    protected ProviderException getDefaultOpenAbortException() {
        // Verify the attach response contained a non-null target
        final Target target = getEndpoint().getRemoteTarget();

        if (target != null) {
            return super.getDefaultOpenAbortException();
        } else {
            // No link terminus was created, the peer has detach/closed us, create IDE.
            return new ProviderInvalidDestinationException("Link creation was refused");
        }
    }
}
