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
import org.apache.qpid.jms.provider.amqp.AmqpConnection;
import org.apache.qpid.jms.provider.amqp.AmqpSession;
import org.apache.qpid.protonj2.engine.Session;

/**
 * AMQP {@link Session} builder to create {@link AmqpSession} instances.
 */
public class AmqpSessionBuilder extends AmqpEndpointBuilder<AmqpSession, AmqpConnection, JmsSessionInfo, Session> {

    public AmqpSessionBuilder(AmqpConnection connection, JmsSessionInfo resourceInfo) {
        super(connection.getProvider(), connection, resourceInfo);
    }

    @Override
    protected Session createEndpoint(JmsSessionInfo resourceInfo) {
        long outgoingWindow = getParent().getProvider().getSessionOutgoingWindow();

        Session session = getParent().getEndpoint().session();
        session.setIncomingCapacity(Integer.MAX_VALUE);
        if (outgoingWindow >= 0) {
            // TODO: session.setOutgoingWindow(outgoingWindow);
        }

        return session;
    }

    @Override
    protected AmqpSession createResource(AmqpConnection connection, JmsSessionInfo resourceInfo, Session session) {
        return new AmqpSession(connection, resourceInfo, session);
    }

    @Override
    protected boolean isClosePending() {
        return false;
    }
}
