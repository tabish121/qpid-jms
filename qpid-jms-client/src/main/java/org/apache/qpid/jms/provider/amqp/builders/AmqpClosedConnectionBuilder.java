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

import java.util.UUID;

import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.amqp.AmqpProvider;
import org.apache.qpid.protonj2.engine.Connection;

/**
 * AMQP {@link Connection} builder that creates and closes the connection immediately
 * to allow completion of a normal open in the case of quick create / destroy of a JMS
 * Connection where SASL negotiations have completed and the remote would be expecting
 * a normal connection open and close to occur.
 */
public class AmqpClosedConnectionBuilder extends AmqpConnectionBuilder {

    public AmqpClosedConnectionBuilder(AmqpProvider provider, JmsConnectionInfo resourceInfo) {
        super(provider, resourceInfo);
    }

    @Override
    protected AsyncResult createRequestIntercepter(final AsyncResult request) {
        return request;
    }

    @Override
    protected void processEndpointRemotelyOpened(Connection connection, JmsConnectionInfo resourceInfo) {
        // Close immediately as we've indicated that a close is pending, this will prompt the
        // remote to actually send a close even if one wasn't incoming.
        getEndpoint().close();
    }

    @Override
    protected Connection createEndpoint(JmsConnectionInfo resourceInfo) {
        if (resourceInfo.getClientId() == null) {
            resourceInfo.setClientId(UUID.randomUUID().toString(), true);
        }

        return super.createEndpoint(resourceInfo);
    }

    @Override
    protected void processEndpointRemotelyClosed(Connection connection, JmsConnectionInfo resourceInfo) {
        // If the resource closed and no error was given, we just closed it now to avoid
        // failing the request with a default error which creates additional logging.
        if (!hasRemoteError()) {
            request.onSuccess();
        }
    }

    @Override
    protected boolean isClosePending() {
        return true;
    }
}
