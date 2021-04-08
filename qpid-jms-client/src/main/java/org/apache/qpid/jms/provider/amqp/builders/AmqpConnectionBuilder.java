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

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.ANONYMOUS_RELAY;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.DELAYED_DELIVERY;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.SHARED_SUBS;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.SOLE_CONNECTION_CAPABILITY;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import javax.jms.Session;

import org.apache.qpid.jms.JmsConnectionExtensions;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;
import org.apache.qpid.jms.provider.amqp.AmqpConnectionProperties;
import org.apache.qpid.jms.provider.amqp.AmqpConnectionSession;
import org.apache.qpid.jms.provider.amqp.AmqpProvider;
import org.apache.qpid.jms.provider.amqp.AmqpRedirect;
import org.apache.qpid.jms.provider.amqp.AmqpSupport;
import org.apache.qpid.jms.provider.exceptions.ProviderConnectionRemotelyClosedException;
import org.apache.qpid.jms.util.MetaDataSupport;
import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.types.Symbol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AMQP {@link Connection} builder implementation.
 */
public class AmqpConnectionBuilder extends AmqpEndpointBuilder<AmqpConnection, AmqpProvider, JmsConnectionInfo, Connection> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConnectionBuilder.class);

    private final AmqpConnectionProperties properties;

    public AmqpConnectionBuilder(AmqpProvider provider, JmsConnectionInfo resourceInfo) {
        super(provider, provider, resourceInfo);

        this.properties = new AmqpConnectionProperties(getResourceInfo(), provider);
    }

    @Override
    public void buildEndpoint(final AsyncResult request) {
        super.buildEndpoint(createRequestIntercepter(request));
    }

    @Override
    public void buildEndpoint(final AsyncResult request, Consumer<AmqpConnection> consumer) {
        super.buildEndpoint(createRequestIntercepter(request), consumer);
    }

    protected AsyncResult createRequestIntercepter(final AsyncResult request) {
        return new AsyncResult() {

            @Override
            public void onSuccess() {
                // Create a Session for this connection that is used for Temporary Destinations
                // and perhaps later on management and advisory monitoring.
                JmsSessionInfo sessionInfo = new JmsSessionInfo(getResourceInfo(), -1);
                sessionInfo.setAcknowledgementMode(Session.AUTO_ACKNOWLEDGE);

                final AmqpConnection connection = getEndpoint().getLinkedResource(AmqpConnection.class);

                final AmqpConnectionSessionBuilder builder = new AmqpConnectionSessionBuilder(connection, sessionInfo);

                builder.buildEndpoint(new AsyncResult() {

                    @Override
                    public boolean isComplete() {
                        return builder.getEndpoint().isRemotelyOpen();
                    }

                    @Override
                    public void onSuccess() {
                        LOG.debug("Connection Session {} is now open: ", getEndpoint());
                        request.onSuccess();
                    }

                    @Override
                    public void onFailure(ProviderException result) {
                        LOG.debug("AMQP Connection Session failed to open.");
                        request.onFailure(result);
                    }

                }, session -> connection.setConnectionSession((AmqpConnectionSession) session));
            }

            @Override
            public void onFailure(ProviderException result) {
                request.onFailure(result);
            }

            @Override
            public boolean isComplete() {
                return getEndpoint().isRemotelyOpen();
            }
        };
    }

    @Override
    protected Connection createEndpoint(JmsConnectionInfo resourceInfo) {
        String hostname = getParent().getVhost();
        if (hostname == null) {
            hostname = getParent().getRemoteURI().getHost();
        } else if (hostname.isEmpty()) {
            hostname = null;
        }

        final Map<Symbol, Object> props = new LinkedHashMap<Symbol, Object>();

        if (resourceInfo.getExtensionMap().containsKey(JmsConnectionExtensions.AMQP_OPEN_PROPERTIES)) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> userConnectionProperties = (Map<String, Object>) resourceInfo.getExtensionMap().get(
                JmsConnectionExtensions.AMQP_OPEN_PROPERTIES).apply(resourceInfo.getConnection(), parent.getTransport().getRemoteLocation());
            if (userConnectionProperties != null && !userConnectionProperties.isEmpty()) {
                userConnectionProperties.forEach((key, value) -> props.put(Symbol.valueOf(key), value));
            }
        }

        // Client properties override anything the user added.
        props.put(AmqpSupport.PRODUCT, MetaDataSupport.PROVIDER_NAME);
        props.put(AmqpSupport.VERSION, MetaDataSupport.PROVIDER_VERSION);
        props.put(AmqpSupport.PLATFORM, MetaDataSupport.PLATFORM_DETAILS);

        Connection connection = getParent().getProtonConnection();
        connection.setHostname(hostname);
        connection.setContainerId(resourceInfo.getClientId());
        connection.setDesiredCapabilities(SOLE_CONNECTION_CAPABILITY, DELAYED_DELIVERY, ANONYMOUS_RELAY, SHARED_SUBS);
        connection.setProperties(props);

        return connection;
    }

    @Override
    protected void processEndpointRemotelyOpened(Connection connection, JmsConnectionInfo info) {
        // Initialize the connection properties so that the state of the remote can
        // be determined, this allows us to check for close pending.
        properties.initialize(connection.getRemoteOfferedCapabilities(), connection.getRemoteProperties());

        // If there are failover servers in the open then we signal that to the listeners
        List<AmqpRedirect> failoverList = properties.getFailoverServerList();
        if (!failoverList.isEmpty()) {
            List<URI> failoverURIs = new ArrayList<>();
            for (AmqpRedirect redirect : failoverList) {
                try {
                    failoverURIs.add(redirect.toURI());
                } catch (Exception ex) {
                    LOG.trace("Error while creating URI from failover server: {}", redirect);
                }
            }
        }
    }

    @Override
    protected AmqpConnection createResource(AmqpProvider parent, JmsConnectionInfo resourceInfo, Connection endpoint) {
        return new AmqpConnection(parent, properties, resourceInfo, endpoint);
    }

    @Override
    protected ProviderException getOpenAbortExceptionFromRemote() {
        return AmqpSupport.convertToConnectionClosedException(getProvider(), getEndpoint().getRemoteCondition());
    }

    @Override
    protected ProviderException getDefaultOpenAbortException() {
        return new ProviderConnectionRemotelyClosedException("Open failed unexpectedly.");
    }

    @Override
    protected boolean isClosePending() {
        return properties.isConnectionOpenFailed();
    }

    @Override
    protected long getOpenTimeout() {
        return getProvider().getConnectTimeout();
    }
}
