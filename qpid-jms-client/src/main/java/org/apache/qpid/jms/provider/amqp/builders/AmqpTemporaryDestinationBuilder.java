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

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.DYNAMIC_NODE_LIFETIME_POLICY;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.TEMP_QUEUE_CREATOR;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.TEMP_TOPIC_CREATOR;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.jms.JmsTemporaryDestination;
import org.apache.qpid.jms.provider.amqp.AmqpSession;
import org.apache.qpid.jms.provider.amqp.AmqpTemporaryDestination;
import org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper;
import org.apache.qpid.protonj2.engine.Sender;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.messaging.DeleteOnClose;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.messaging.Target;
import org.apache.qpid.protonj2.types.messaging.TerminusDurability;
import org.apache.qpid.protonj2.types.messaging.TerminusExpiryPolicy;
import org.apache.qpid.protonj2.types.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.types.transport.SenderSettleMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AmqpTemporaryDestination} builder object that create a {@link Sender} link with a dynamic
 * node in order to generate a the temporary destination at the remote peer.
 */
public class AmqpTemporaryDestinationBuilder extends AmqpEndpointBuilder<AmqpTemporaryDestination, AmqpSession, JmsTemporaryDestination, Sender> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpTemporaryDestinationBuilder.class);

    public AmqpTemporaryDestinationBuilder(AmqpSession session, JmsTemporaryDestination resourceInfo) {
        super(session.getProvider(), session, resourceInfo);
    }

    @Override
    protected Sender createEndpoint(JmsTemporaryDestination resourceInfo) {
        // Form a link name, use the local generated name with a prefix to aid debugging
        String localDestinationName = resourceInfo.getAddress();
        String senderLinkName = null;
        if (resourceInfo.isQueue()) {
            senderLinkName = "qpid-jms:" + TEMP_QUEUE_CREATOR + localDestinationName;
        } else {
            senderLinkName = "qpid-jms:" + TEMP_TOPIC_CREATOR + localDestinationName;
        }

        // Just use a bare Source, this is a producer which
        // wont send anything and the link name is unique.
        Source source = new Source();

        Target target = new Target();
        target.setDynamic(true);
        target.setDurable(TerminusDurability.NONE);
        target.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);

        // Set the dynamic node lifetime-policy
        Map<Symbol, Object> dynamicNodeProperties = new HashMap<Symbol, Object>();
        dynamicNodeProperties.put(DYNAMIC_NODE_LIFETIME_POLICY, DeleteOnClose.getInstance());
        target.setDynamicNodeProperties(dynamicNodeProperties);

        // Set the capability to indicate the node type being created
        if (resourceInfo.isQueue()) {
            target.setCapabilities(AmqpDestinationHelper.TEMP_QUEUE_CAPABILITY);
        } else {
            target.setCapabilities(AmqpDestinationHelper.TEMP_TOPIC_CAPABILITY);
        }

        Sender sender = getParent().getEndpoint().sender(senderLinkName);
        sender.setSource(source);
        sender.setTarget(target);
        sender.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        sender.setReceiverSettleMode(ReceiverSettleMode.FIRST);

        return sender;
    }

    @Override
    protected AmqpTemporaryDestination createResource(AmqpSession session, JmsTemporaryDestination resourceInfo, Sender sender) {
        return new AmqpTemporaryDestination(session, resourceInfo, sender);
    }

    @Override
    protected boolean isClosePending() {
        // When no link terminus was created, the peer will now detach/close us otherwise
        // we need to validate the returned remote source prior to open completion.
        return getEndpoint().getRemoteTarget() == null;
    }

    @Override
    protected void processEndpointRemotelyOpened(Sender sender, JmsTemporaryDestination resourceInfo) {
        if (!isClosePending()) {
            // Once our sender is opened we can read the updated name from the target address.
            String oldDestinationName = resourceInfo.getAddress();
            String destinationName = sender.<Target>getRemoteTarget().getAddress();

            resourceInfo.setAddress(destinationName);

            LOG.trace("Updated temp destination to: {} from: {}", destinationName, oldDestinationName);
        }
    }
}
