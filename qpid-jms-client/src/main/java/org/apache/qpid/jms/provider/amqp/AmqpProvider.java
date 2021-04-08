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
import java.net.URI;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;
import javax.security.sasl.SaslException;

import org.apache.qpid.jms.JmsConnectionExtensions;
import org.apache.qpid.jms.JmsTemporaryDestination;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsMessageFactory;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsConsumerInfo;
import org.apache.qpid.jms.meta.JmsDefaultResourceVisitor;
import org.apache.qpid.jms.meta.JmsProducerId;
import org.apache.qpid.jms.meta.JmsProducerInfo;
import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.meta.JmsResourceVistor;
import org.apache.qpid.jms.meta.JmsSessionId;
import org.apache.qpid.jms.meta.JmsSessionInfo;
import org.apache.qpid.jms.meta.JmsTransactionInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.provider.ProviderFutureFactory;
import org.apache.qpid.jms.provider.ProviderListener;
import org.apache.qpid.jms.provider.ProviderSynchronization;
import org.apache.qpid.jms.provider.amqp.builders.AmqpClosedConnectionBuilder;
import org.apache.qpid.jms.provider.amqp.builders.AmqpConnectionBuilder;
import org.apache.qpid.jms.provider.exceptions.ProviderClosedException;
import org.apache.qpid.jms.provider.exceptions.ProviderConnectionSecuritySaslException;
import org.apache.qpid.jms.provider.exceptions.ProviderExceptionSupport;
import org.apache.qpid.jms.provider.exceptions.ProviderFailedException;
import org.apache.qpid.jms.provider.exceptions.ProviderIOException;
import org.apache.qpid.jms.provider.exceptions.ProviderIllegalStateException;
import org.apache.qpid.jms.provider.exceptions.ProviderOperationTimedOutException;
import org.apache.qpid.jms.provider.exceptions.ProviderTransactionInDoubtException;
import org.apache.qpid.jms.sasl.SaslMechanismFinder;
import org.apache.qpid.jms.sasl.SaslSecurityRuntimeException;
import org.apache.qpid.jms.transports.Transport;
import org.apache.qpid.jms.transports.TransportListener;
import org.apache.qpid.jms.util.PropertyUtil;
import org.apache.qpid.jms.util.QpidJMSThreadFactory;
import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.engine.ConnectionState;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.EngineSaslDriver.SaslState;
import org.apache.qpid.protonj2.engine.exceptions.EngineStateException;
import org.apache.qpid.protonj2.engine.exceptions.FrameDecodingException;
import org.apache.qpid.protonj2.engine.exceptions.FrameEncodingException;
import org.apache.qpid.protonj2.engine.exceptions.MalformedAMQPHeaderException;
import org.apache.qpid.protonj2.engine.impl.ProtonEngineFactory;
import org.apache.qpid.protonj2.engine.sasl.SaslOutcome;
import org.apache.qpid.protonj2.engine.sasl.client.SaslAuthenticator;
import org.apache.qpid.protonj2.engine.sasl.client.SaslCredentialsProvider;
import org.apache.qpid.protonj2.engine.sasl.client.SaslMechanismSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An AMQP v1.0 Provider.
 *
 * The AMQP Provider is bonded to a single remote broker instance.  The provider will attempt
 * to connect to only that instance and once failed can not be recovered.  For clients that
 * wish to implement failover type connections a new AMQP Provider instance must be created
 * and state replayed from the JMS layer using the standard recovery process defined in the
 * JMS Provider API.
 *
 * All work within this Provider is serialized to a single Thread.  Any asynchronous exceptions
 * will be dispatched from that Thread and all in-bound requests are handled there as well.
 */
public class AmqpProvider implements Provider, TransportListener {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpProvider.class);

    private static final int DEFAULT_MAX_FRAME_SIZE = 1024 * 1024 * 1;
    private static final int DEFAULT_CHANNEL_MAX = 65535;
    private static final AtomicInteger PROVIDER_SEQUENCE = new AtomicInteger();

    private static final int DEFAULT_MAX_WRITE_BYTES_BEFORE_FLUSH = 128 * 1024;
    private static final int DEFAULT_ANONYMOUS_FALLBACK_CACHE_TIMEOUT = 30000;
    private static final int DEFAULT_ANONYMOUS_FALLBACK_CACHE_SIZE = 1;

    private volatile ProviderListener listener;
    private volatile AmqpConnection connection;
    private final Transport transport;
    private String vhost;
    private boolean traceBytes;
    private boolean traceFrames;
    private boolean autoFlush = true;
    private boolean saslLayer = true;
    private Set<String> saslMechanisms;
    private JmsConnectionInfo connectionInfo;
    private int idleTimeout = 60000;
    private int drainTimeout = 60000;
    private long sessionOutoingWindow = -1; // Use proton default
    private int maxFrameSize = DEFAULT_MAX_FRAME_SIZE;
    private int channelMax = DEFAULT_CHANNEL_MAX;
    private int maxWriteBytesBeforeFlush = DEFAULT_MAX_WRITE_BYTES_BEFORE_FLUSH;
    private int anonymousFallbackCacheTimeout = DEFAULT_ANONYMOUS_FALLBACK_CACHE_TIMEOUT;
    private int anonymousFallbackCacheSize = DEFAULT_ANONYMOUS_FALLBACK_CACHE_SIZE;

    private boolean allowNonSecureRedirects;

    private final URI remoteURI;
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile Throwable failureCause;
    private ScheduledExecutorService serializer;
    private Engine protonEngine;
    private Connection protonConnection;

    private final ProviderFutureFactory futureFactory;
    private AsyncResult connectionRequest;
    private ScheduledFuture<?> nextIdleTimeoutCheck;

    /**
     * Create a new instance of an AmqpProvider bonded to the given remote URI.
     *
     * @param remoteURI
     *        The URI of the AMQP broker this Provider instance will connect to.
     * @param transport
     * 		  The underlying Transport that will be used for wire level communications.
     * @param futureFactory
     * 		  The ProviderFutureFactory to use when futures are requested.
     */
    public AmqpProvider(URI remoteURI, Transport transport, ProviderFutureFactory futureFactory) {
        this.remoteURI = remoteURI;
        this.transport = transport;
        this.futureFactory = futureFactory;
    }

    @Override
    public void connect(final JmsConnectionInfo connectionInfo) throws ProviderException {
        checkClosedOrFailed();

        if (serializer != null) {
            throw new IllegalStateException("Connect cannot be called more than once");
        }

        final ProviderFuture connectRequest = futureFactory.createFuture();

        // Configure Transport prior to initialization at which point configuration is set and
        // cannot be updated.  All further interaction should take place on the serializer for
        // thread safety.

        final SSLContext sslContextOverride = configureTransport(connectionInfo);

        try {
            serializer = transport.connect(() -> {
                this.connectionInfo = connectionInfo;
                this.connectionRequest = connectRequest;

                if (saslLayer) {
                    protonEngine = ProtonEngineFactory.PROTON.createEngine();
                } else {
                    protonEngine = ProtonEngineFactory.PROTON.createNonSaslEngine();
                }

                protonEngine.configuration().setBufferAllocator(transport.getBufferAllocator());
                protonEngine.outputHandler(this::handleEngineOutput)
                            .shutdownHandler(this::handleEngineShutdown)
                            .errorHandler(this::handleEngineFailure);

                configureEngineSaslSupport();

                protonConnection = protonEngine.start();
                protonConnection.setIdleTimeout(getIdleTimeout());
                if (getMaxFrameSize() > 0) {
                    protonConnection.setMaxFrameSize(getMaxFrameSize());
                }
            }, sslContextOverride);

            protonConnection.negotiate(amqpHeader -> connectRequest.onSuccess());

        } catch (Throwable t) {
            connectRequest.onFailure(ProviderExceptionSupport.createOrPassthroughFatal(t));
        }

        if (connectionInfo.getConnectTimeout() != JmsConnectionInfo.INFINITE) {
            if (!connectRequest.sync(connectionInfo.getConnectTimeout(), TimeUnit.MILLISECONDS)) {
                throw new ProviderOperationTimedOutException("Timed out while waiting to connect");
            }
        } else {
            connectRequest.sync();
        }
    }

    @Override
    public void start() throws ProviderException, IllegalStateException {
        checkClosedOrFailed();

        if (listener == null) {
            throw new IllegalStateException("No ProviderListener registered.");
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            final ProviderFuture request = futureFactory.createUnfailableFuture();

            // Possible that the connect call failed before calling transport connect or the connect
            // call failed and shutdown the event loop in which case we have no work to do other than
            // to clean up the transport by closing it down.
            if (serializer != null && !serializer.isShutdown()) {
                try {
                    serializer.execute(() -> {
                        try {
                            // If we are not connected then there is nothing we can do now
                            // just signal success.
                            if (transport == null || !transport.isConnected()) {
                                request.onSuccess();
                                return;
                            }

                            if (connection != null) {
                                connection.close(request);
                            } else {
                                // If the SASL authentication occurred but failed then we don't
                                // need to do an open / close
                                if (protonEngine.saslDriver().getSaslState() == SaslState.AUTHENTICATION_FAILED) {
                                    request.onSuccess();
                                    return;
                                }

                                // Connection attempt might have been tried and failed so only perform
                                // an open / close cycle if one hasn't been done already.
                                if (protonConnection.getState() == ConnectionState.IDLE) {
                                    new AmqpClosedConnectionBuilder(this, connectionInfo).buildEndpoint(request);
                                } else {
                                    request.onSuccess();
                                }
                            }
                        } catch (Exception e) {
                            LOG.debug("Caught exception while closing proton connection: {}", e.getMessage());
                        } finally {
                            if (nextIdleTimeoutCheck != null) {
                                LOG.trace("Cancelling scheduled IdleTimeoutCheck");
                                nextIdleTimeoutCheck.cancel(false);
                                nextIdleTimeoutCheck = null;
                            }
                        }
                    });
                } catch (RejectedExecutionException rje) {
                    // Transport likely encountered some critical error on connect and the executor
                    // resource is not initialized now, in which case just ignore and continue on.
                    LOG.trace("Close of provider resources was rejected from Transport IO thread: ", rje);
                    request.onSuccess();
                }
            } else {
                request.onSuccess();
            }

            try {
                if (getCloseTimeout() < 0) {
                    request.sync();
                } else {
                    request.sync(getCloseTimeout(), TimeUnit.MILLISECONDS);
                }
            } catch (ProviderException e) {
                LOG.warn("Error caught while closing Provider: {}", e.getMessage() != null ? e.getMessage() : "<Unknown Error>");
            } finally {
                if (transport != null) {
                    try {
                        transport.close();
                    } catch (Exception e) {
                        LOG.debug("Caught exception while closing down Transport: {}", e.getMessage());
                    }
                }
            }
        }
    }

    @Override
    public void create(final JmsResource resource, final AsyncResult request) throws ProviderException {
        checkClosedOrFailed();
        checkConnected();

        serializer.execute(() -> {

            try {
                checkClosedOrFailed();
                resource.visit(new JmsResourceVistor() {
                    @Override
                    public void processSessionInfo(JmsSessionInfo sessionInfo) throws Exception {
                        connection.createSession(sessionInfo, request);
                    }

                    @Override
                    public void processProducerInfo(JmsProducerInfo producerInfo) throws Exception {
                        AmqpSession session = connection.getSession(producerInfo.getParentId());
                        session.createProducer(producerInfo, request);
                    }

                    @Override
                    public void processConsumerInfo(JmsConsumerInfo consumerInfo) throws Exception {
                        final AmqpSession session;

                        if (consumerInfo.isConnectionConsumer()) {
                            session = connection.getConnectionSession();
                        } else {
                            session = connection.getSession(consumerInfo.getParentId());
                        }

                        session.createConsumer(consumerInfo, request);
                    }

                    @Override
                    public void processConnectionInfo(JmsConnectionInfo connectionInfo) throws Exception {
                        AmqpProvider.this.connectionInfo = connectionInfo;

                        AmqpConnectionBuilder builder = new AmqpConnectionBuilder(AmqpProvider.this, connectionInfo);
                        connectionRequest = new AsyncResult() {
                            @Override
                            public void onSuccess() {
                                fireConnectionEstablished();
                                request.onSuccess();
                            }

                            @Override
                            public void onFailure(ProviderException result) {
                                request.onFailure(result);
                            }

                            @Override
                            public boolean isComplete() {
                                return request.isComplete();
                            }
                        };

                        builder.buildEndpoint(connectionRequest, resource -> AmqpProvider.this.connection = resource);
                    }

                    @Override
                    public void processDestination(JmsTemporaryDestination destination) throws Exception {
                        if (destination.isTemporary()) {
                            connection.createTemporaryDestination(destination, request);
                        } else {
                            request.onSuccess();
                        }
                    }

                    @Override
                    public void processTransactionInfo(JmsTransactionInfo transactionInfo) throws Exception {
                        AmqpSession session = connection.getSession(transactionInfo.getSessionId());
                        session.begin(transactionInfo.getId(), request);
                    }
                });
            } catch (Throwable t) {
                request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(t));
            }
        });
    }

    @Override
    public void start(final JmsResource resource, final AsyncResult request) throws ProviderException {
        checkClosedOrFailed();
        checkConnected();

        serializer.execute(() -> {

            try {
                checkClosedOrFailed();
                resource.visit(new JmsDefaultResourceVisitor() {

                    @Override
                    public void processConsumerInfo(JmsConsumerInfo consumerInfo) throws Exception {
                        final AmqpSession session;
                        if (consumerInfo.isConnectionConsumer()) {
                            session = connection.getConnectionSession();
                        } else {
                            session = connection.getSession(consumerInfo.getParentId());
                        }

                        session.getConsumer(consumerInfo).start(request);
                    }
                });
            } catch (Throwable t) {
                request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(t));
            }
        });
    }

    @Override
    public void stop(final JmsResource resource, final AsyncResult request) throws ProviderException {
        checkClosedOrFailed();
        checkConnected();

        serializer.execute(() -> {

            try {
                checkClosedOrFailed();
                resource.visit(new JmsDefaultResourceVisitor() {

                    @Override
                    public void processConsumerInfo(JmsConsumerInfo consumerInfo) throws Exception {
                        final AmqpSession session;
                        if (consumerInfo.isConnectionConsumer()) {
                            session = connection.getConnectionSession();
                        } else {
                            session = connection.getSession(consumerInfo.getParentId());
                        }

                        session.getConsumer(consumerInfo).stop(request);
                    }
                });
            } catch (Throwable t) {
                request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(t));
            }
        });
    }

    @Override
    public void destroy(final JmsResource resource, final AsyncResult request) throws ProviderException {
        checkClosedOrFailed();
        checkConnected();

        serializer.execute(() -> {

            try {
                checkClosedOrFailed();
                resource.visit(new JmsDefaultResourceVisitor() {

                    @Override
                    public void processSessionInfo(JmsSessionInfo sessionInfo) throws Exception {
                        connection.getSession(sessionInfo.getId()).close(request);
                    }

                    @Override
                    public void processProducerInfo(JmsProducerInfo producerInfo) throws Exception {
                        AmqpSession session = connection.getSession(producerInfo.getParentId());
                        AmqpProducer producer = session.getProducer(producerInfo);
                        producer.close(request);
                    }

                    @Override
                    public void processConsumerInfo(final JmsConsumerInfo consumerInfo) throws Exception {
                        final AmqpSession session = connection.getSession(consumerInfo.getParentId());
                        session.getConsumer(consumerInfo).close(request);
                    }

                    @Override
                    public void processConnectionInfo(JmsConnectionInfo connectionInfo) throws Exception {
                        connection.close(request);
                    }

                    @Override
                    public void processDestination(JmsTemporaryDestination destination) throws Exception {
                        AmqpTemporaryDestination temporary = connection.getTemporaryDestination(destination);
                        if (temporary != null) {
                            temporary.close(request);
                        } else {
                            LOG.debug("Could not find temporary destination {} to delete.", destination);
                            request.onSuccess();
                        }
                    }
                });
            } catch (Throwable t) {
                request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(t));
            }
        });
    }

    @Override
    public void send(final JmsOutboundMessageDispatch envelope, final AsyncResult request) throws ProviderException {
        checkClosedOrFailed();
        checkConnected();

        serializer.execute(() -> {

            try {
                checkClosedOrFailed();
                JmsProducerId producerId = envelope.getProducerId();
                AmqpProducer producer = (AmqpProducer) producerId.getProviderHint();
                producer.send(envelope, request);
            } catch (Throwable t) {
                request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(t));
            }
        });
    }

    @Override
    public void acknowledge(final JmsSessionId sessionId, final ACK_TYPE ackType, final AsyncResult request) throws ProviderException {
        checkClosedOrFailed();
        checkConnected();

        serializer.execute(() -> {

            try {
                checkClosedOrFailed();
                AmqpSession amqpSession = (AmqpSession) sessionId.getProviderHint();
                if (amqpSession != null) {
                    amqpSession.acknowledge(ackType);
                    request.onSuccess();
                } else {
                    throw new ProviderIllegalStateException("Cannot acknowledge message from session that does not exist.");
                }
            } catch (Throwable t) {
                request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(t));
            }
        });
    }

    @Override
    public void acknowledge(final JmsInboundMessageDispatch envelope, final ACK_TYPE ackType, final AsyncResult request) throws ProviderException {
        checkClosedOrFailed();
        checkConnected();

        serializer.execute(() -> {

            try {
                checkClosedOrFailed();

                JmsConsumerId consumerId = envelope.getConsumerId();
                AmqpConsumer consumer = (AmqpConsumer) consumerId.getProviderHint();

                if (consumer.getSession().isAsyncAck()) {
                    request.onSuccess();
                    consumer.acknowledge(envelope, ackType);
                } else {
                    autoFlush = false;
                    consumer.acknowledge(envelope, ackType);
                    request.onSuccess();
                    transport.flush();
                }
            } catch (Throwable t) {
                request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(t));
            } finally {
                autoFlush = true;
            }
        });
    }

    @Override
    public void commit(final JmsTransactionInfo transactionInfo, JmsTransactionInfo nextTransactionId, final AsyncResult request) throws ProviderException {
        checkClosedOrFailed();
        checkConnected();

        serializer.execute(() -> {

            try {
                checkClosedOrFailed();
                AmqpSession session = connection.getSession(transactionInfo.getSessionId());
                if (session != null) {
                    session.commit(transactionInfo, nextTransactionId, request);
                } else {
                    if (transactionInfo.isInDoubt()) {
                        throw new ProviderTransactionInDoubtException("Commit of in-doubt transaction failed because no session exists");
                    } else {
                        throw new ProviderIllegalStateException("Commit of transaction failed because no session exists");
                    }
                }
            } catch (Throwable t) {
                request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(t));
            }
        });
    }

    @Override
    public void rollback(final JmsTransactionInfo transactionInfo, JmsTransactionInfo nextTransactionId, final AsyncResult request) throws ProviderException {
        checkClosedOrFailed();
        checkConnected();

        serializer.execute(() -> {

            try {
                checkClosedOrFailed();
                AmqpSession session = connection.getSession(transactionInfo.getSessionId());
                if (session != null) {
                    session.rollback(transactionInfo, nextTransactionId, request);
                } else {
                    if (transactionInfo.isInDoubt()) {
                        throw new ProviderTransactionInDoubtException("Rollback of in-doubt transaction failed because no session exists");
                    } else {
                        throw new ProviderIllegalStateException("Rollback of transaction failed because no session exists");
                    }
                }
            } catch (Throwable t) {
                request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(t));
            }
        });
    }

    @Override
    public void recover(final JmsSessionId sessionId, final AsyncResult request) throws ProviderException {
        checkClosedOrFailed();
        checkConnected();

        serializer.execute(() -> {

            try {
                checkClosedOrFailed();
                AmqpSession session = (AmqpSession) sessionId.getProviderHint();
                if (session != null) {
                    session.recover();
                    request.onSuccess();
                } else {
                    throw new ProviderIllegalStateException("Cannot recover messages from session that does not exist");
                }
            } catch (Throwable t) {
                request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(t));
            }
        });
    }

    @Override
    public void unsubscribe(final String subscription, final AsyncResult request) throws ProviderException {
        checkClosedOrFailed();
        checkConnected();

        serializer.execute(() -> {

            try {
                checkClosedOrFailed();
                connection.unsubscribe(subscription, request);
            } catch (Throwable t) {
                request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(t));
            }
        });
    }

    @Override
    public void pull(final JmsConsumerId consumerId, final long timeout, final AsyncResult request) throws ProviderException {
        checkClosedOrFailed();
        checkConnected();

        serializer.execute(() -> {

            try {
                checkClosedOrFailed();
                AmqpConsumer consumer = (AmqpConsumer) consumerId.getProviderHint();
                consumer.pull(timeout, request);
            } catch (Throwable t) {
                request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(t));
            }
        });
    }

    //---------- Event handlers and Utility methods  -------------------------//

    public void submitToEventLoop(Runnable task) {
        serializer.execute(() -> {
            try {
                task.run();
            } catch (Throwable t) {
                LOG.warn("Caught problem during task processing: {}", t.getMessage(), t);
                fireProviderException(ProviderExceptionSupport.createNonFatalOrPassthrough(t));
            }
        });
    }

    @Override
    public void onData(final ProtonBuffer input) {
        try {
            if (isTraceBytes()) {
                // TODO TRACE_BYTES.info("Received: {}", ByteBufUtil.hexDump(input));
            }

            do {
                protonEngine.ingest(input);
            } while (input.isReadable() && protonEngine.isWritable());
            // TODO - How do we handle case of not all data read ?
        } catch (EngineStateException e) {
            LOG.warn("Caught problem during data processing: {}", e.getMessage(), e);
            fireProviderException(ProviderExceptionSupport.createOrPassthroughFatal(e));
        }
    }

    /**
     * Callback method for the Transport to report connection errors.  When called
     * the method will queue a new task to fire the failure error back to the listener.
     *
     * @param error
     *        the error that causes the transport to fail.
     */
    @Override
    public void onTransportError(final Throwable error) {
        if (!serializer.isShutdown()) {
            serializer.execute(() -> {
                LOG.info("Transport failed: {}", error.getMessage());
                if (!closed.get()) {
                    // We can't send any more output, so close the transport
                    protonEngine.shutdown();
                    fireProviderException(ProviderExceptionSupport.createOrPassthroughFatal(error));
                }
            });
        }
    }

    /**
     * Callback method for the Transport to report that the underlying connection
     * has closed.  When called this method will queue a new task that will check for
     * the closed state on this transport and if not closed then an exception is raised
     * to the registered ProviderListener to indicate connection loss.
     */
    @Override
    public void onTransportClosed() {
        if (!serializer.isShutdown()) {
            serializer.execute(() -> {
                LOG.debug("Transport connection remotely closed");
                if (!closed.get()) {
                    // We can't send any more output, so fail the engine which will result in
                    // provider shutdown and cleanup processing and an eventual signal to any
                    // registered provider listener that there was a fatal error.
                    protonEngine.engineFailed(new ProviderFailedException("Transport connection remotely closed."));
                }
            });
        }
    }

    private void handleEngineOutput(ProtonBuffer output) {
        try {
            if (autoFlush) {
                transport.writeAndFlush(output);
            } else {
                transport.write(output);
            }
        } catch (IOException e) {
            LOG.warn("Error while writing engine output to transport:", e);
            throw new UncheckedIOException(e);
        }
    }

    private void handleEngineFailure(Engine engine) {
        LOG.trace("Engine failre reported, processing for shutdown.");
        Throwable cause = engine.failureCause();
        if (cause instanceof SaslException) {
            failureCause = new ProviderConnectionSecuritySaslException(
                cause.getMessage(), protonEngine.saslDriver().getSaslOutcome().ordinal(), cause);
        } else if (cause instanceof MalformedAMQPHeaderException) {
            if (saslLayer && protonEngine.saslDriver().getSaslOutcome() != SaslOutcome.SASL_OK) {
                failureCause = new ProviderIOException("SASL header mismatch", cause);
            } else {
                failureCause = new ProviderIOException("AMQP header mismatch", cause);
            }
        } else if (cause instanceof FrameEncodingException || cause instanceof FrameDecodingException) {
            failureCause = new ProviderFailedException(cause.getMessage(), cause);
        } else if (failureCause == null) {
            failureCause = ProviderExceptionSupport.createOrPassthroughFatal(cause);
        }

        protonEngine.shutdown();
    }

    private void handleEngineShutdown(Engine engine) {
        LOG.trace("Engine shutdown handler running now, closed state: {}", closed.get());
        if (closed.compareAndSet(false, true)) {
            // Shut down outside of close processing so we always signal failure of some sort.
            if (failureCause == null) {
                if (engine.failureCause() != null) {
                    failureCause = ProviderExceptionSupport.createOrPassthroughFatal(engine.failureCause());
                } else {
                    failureCause = new ProviderFailedException("Provider AMQP Engine shutdown for unknown reason.");
                }
            }

            try {
                protonConnection.close();
            } catch (Exception ignore) {
            }

            try {
                transport.close();
            } catch (IOException ignored) {
            }

            ProviderException pex = ProviderExceptionSupport.createOrPassthroughFatal(failureCause);
            if (connectionRequest != null) {
                connectionRequest.onFailure(pex);
                connectionRequest = null;
            } else {
                fireProviderException(pex);
            }
        }
    }

    void fireConnectionEstablished() {
        // The request onSuccess calls this method
        connectionRequest = null;

        // Using nano time since it is not related to the wall clock, which may change
        long now = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        long deadline = protonEngine.tick(now);
        if (deadline != 0) {
            long delay = deadline - now;
            LOG.trace("IdleTimeoutCheck being initiated, initial delay: {}", delay);
            nextIdleTimeoutCheck = serializer.schedule(new IdleTimeoutCheck(), delay, TimeUnit.MILLISECONDS);
        }

        ProviderListener listener = this.listener;
        if (listener != null) {
            listener.onConnectionEstablished(remoteURI);
        }
    }

    void fireNonFatalProviderException(ProviderException ex) {
        ProviderListener listener = this.listener;
        if (listener != null) {
            listener.onProviderException(ex);
        }
    }

    void fireProviderException(ProviderException ex) {
        if (connectionRequest != null) {
            connectionRequest.onFailure(ex);
            connectionRequest = null;
        }

        if (nextIdleTimeoutCheck != null) {
            nextIdleTimeoutCheck.cancel(true);
            nextIdleTimeoutCheck = null;
        }

        failureCause = ex;

        ProviderListener listener = this.listener;
        if (listener != null) {
            listener.onConnectionFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(ex));
        }
    }

    void fireResourceClosed(JmsResource resource, ProviderException cause) {
        ProviderListener listener = this.listener;
        if (listener != null) {
            listener.onResourceClosed(resource, cause);
        }
    }

    //---------- Property Setters and Getters --------------------------------//

    @Override
    public JmsMessageFactory getMessageFactory() {
        if (connection == null) {
            throw new RuntimeException("Message Factory is not accessible when not connected.");
        }
        return connection.getAmqpMessageFactory();
    }

    @Override
    public ProviderFuture newProviderFuture() {
        return futureFactory.createFuture();
    }

    @Override
    public ProviderFuture newProviderFuture(ProviderSynchronization synchronization) {
        return futureFactory.createFuture(synchronization);
    }

    public void setTraceBytes(boolean trace) {
        this.traceBytes = trace;
    }

    public boolean isTraceBytes() {
        return this.traceBytes;
    }

    public boolean isTraceFrames() {
        return traceFrames;
    }

    public void setTraceFrames(boolean traceFrames) {
        this.traceFrames = traceFrames;
    }

    public boolean isSaslLayer() {
        return saslLayer;
    }

    /**
     * Sets whether a sasl layer is used for the connection or not.
     *
     * @param saslLayer true to enable the sasl layer, false to disable it.
     */
    public void setSaslLayer(boolean saslLayer) {
        this.saslLayer = saslLayer;
    }

    public Set<String> getSaslMechanisms() {
        return saslMechanisms;
    }

    /**
     * Sets a selection of mechanisms to restrict the choice to, enabling only
     * a subset of the servers offered mechanisms to be selectable.
     *
     * @param saslMechanisms the mechanisms to restrict choice to, or null not to restrict.
     */
    public void setSaslMechanisms(String[] saslMechanisms) {
        Set<String> saslMechanismSet = null;

        if (saslMechanisms != null && saslMechanisms.length > 0) {
            Set<String> mechs = new HashSet<String>();
            for (int i = 0; i < saslMechanisms.length; i++) {
                String mech = saslMechanisms[i];
                if (!mech.trim().isEmpty()) {
                    mechs.add(mech);
                }
            }

            if (!mechs.isEmpty()) {
                saslMechanismSet = mechs;
            }
        }

        this.saslMechanisms = saslMechanismSet;
    }

    public String getVhost() {
        return vhost;
    }

    /**
     * Sets the hostname to be used in the AMQP SASL Init and Open frames.
     *
     * If set null, the host provided in the remoteURI will be used. If set to
     * the empty string, the hostname field of the frames will be cleared.
     *
     * @param vhost the hostname to include in SASL Init and Open frames.
     */
    public void setVhost(String vhost) {
        this.vhost = vhost;
    }

    public int getIdleTimeout() {
        return idleTimeout;
    }

    /**
     * Sets the idle timeout (in milliseconds) after which the connection will
     * be closed if the peer has not send any data. The provided value will be
     * halved before being transmitted as our advertised idle-timeout in the
     * AMQP Open frame.
     *
     * @param idleTimeout the timeout in milliseconds.
     */
    public void setIdleTimeout(int idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public int getDrainTimeout() {
        return drainTimeout;
    }

    /**
     * Sets the drain timeout (in milliseconds) after which a consumer will be
     * treated as having failed and will be closed due to unknown state of the
     * remote having not responded to the requested drain.
     *
     * @param drainTimeout
     *      the drainTimeout to use for receiver links.
     */
    public void setDrainTimeout(int drainTimeout) {
        this.drainTimeout = drainTimeout;
    }

    public int getMaxFrameSize() {
        return maxFrameSize;
    }

    public int getMaxWriteBytesBeforeFlush() {
        return maxWriteBytesBeforeFlush;
    }

    public int getChannelMax() {
        return channelMax;
    }

    public void setChannelMax(int channelMax) {
        this.channelMax = channelMax;
    }

    /**
     * Sets the maximum number of bytes that will be written on a large set of batched writes
     * before a flush is requested on the {@link Transport}.
     *
     * @param maxWriteBytesBeforeFlush
     * 		number of bytes written before a flush is requested.
     */
    public void setMaxWriteBytesBeforeFlush(int maxWriteBytesBeforeFlush) {
        this.maxWriteBytesBeforeFlush = maxWriteBytesBeforeFlush;
    }

    /**
     * @return the configured max number of cached anonymous fallback producers to keep.
     */
    public int getAnonymousFallbackCacheSize() {
        return anonymousFallbackCacheSize;
    }

    /**
     * Sets the number of anonymous fallback producers to keep open in a cache in order to improve
     * overall performance of anonymous fallback producer sends.
     *
     * @param size
     * 		The number of fallback producers to cache.
     */
    public void setAnonymousFallbackCacheSize(int size) {
        this.anonymousFallbackCacheSize = size;
    }

    /**
     * @return The configured time before a cache anonymous producer link is close due to inactivity.
     */
    public int getAnonymousFallbackCacheTimeout() {
        return anonymousFallbackCacheTimeout;
    }

    /**
     * Sets the timeout used to close cached anonymous producers that have not sent any messages in that
     * time period.  The value is set in milliseconds with a value less that or equal to zero resulting in
     * no timeout being applied.
     *
     * @param timeout
     * 		Time in milliseconds that a cache anonymous producer can be idle before being close.
     */
    public void setAnonymousFallbackCacheTimeout(int timeout) {
        this.anonymousFallbackCacheTimeout = timeout;
    }

    /**
     * Sets the max frame size (in bytes).
     *
     * Values of -1 indicates to use the proton default.
     *
     * @param maxFrameSize the frame size in bytes.
     */
    public void setMaxFrameSize(int maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    public long getSessionOutgoingWindow() {
        return sessionOutoingWindow;
    }

    /**
     * Sets the outgoing window size for the AMQP session. Values may
     * be between -1 and 2^32-1, where -1 indicates to use the default.
     *
     * @param sessionOutoingWindow the outgoing window size
     */
    public void setSessionOutgoingWindow(long sessionOutoingWindow) {
        this.sessionOutoingWindow = sessionOutoingWindow;
    }

    public boolean isAllowNonSecureRedirects() {
        return allowNonSecureRedirects;
    }

    /**
     * Should the AMQP connection allow a redirect or failover server update that redirects
     * from a secure connection to an non-secure one (SSL to TCP).
     *
     * @param allowNonSecureRedirects
     * 		the allowNonSecureRedirects value to apply to this AMQP connection.
     */
    public void setAllowNonSecureRedirects(boolean allowNonSecureRedirects) {
        this.allowNonSecureRedirects = allowNonSecureRedirects;
    }

    public long getCloseTimeout() {
        return connectionInfo != null ? connectionInfo.getCloseTimeout() : JmsConnectionInfo.DEFAULT_CLOSE_TIMEOUT;
    }

    public long getConnectTimeout() {
        return connectionInfo != null ? connectionInfo.getConnectTimeout() : JmsConnectionInfo.DEFAULT_CONNECT_TIMEOUT;
    }

    public long getRequestTimeout() {
        return connectionInfo != null ? connectionInfo.getRequestTimeout() : JmsConnectionInfo.DEFAULT_REQUEST_TIMEOUT;
    }

    public long getSendTimeout() {
        return connectionInfo != null ? connectionInfo.getSendTimeout() : JmsConnectionInfo.DEFAULT_SEND_TIMEOUT;
    }

    @Override
    public String toString() {
        return "AmqpProvider: " + getRemoteURI().getHost() + ":" + getRemoteURI().getPort();
    }

    public Transport getTransport() {
        return transport;
    }

    @Override
    public void setProviderListener(ProviderListener listener) {
        this.listener = listener;
    }

    @Override
    public ProviderListener getProviderListener() {
        return listener;
    }

    @Override
    public URI getRemoteURI() {
        return remoteURI;
    }

    public Throwable getFailureCause() {
        return failureCause;
    }

    public boolean isFailed() {
        return failureCause != null;
    }

    @Override
    public List<URI> getAlternateURIs() {
        List<URI> alternates = new ArrayList<>();

        if (connection != null) {
            // If there are failover servers in the open then we signal that to the listeners
            List<AmqpRedirect> failoverList = connection.getProperties().getFailoverServerList();
            if (!failoverList.isEmpty()) {
                for (AmqpRedirect redirect : failoverList) {
                    try {
                        alternates.add(redirect.toURI());
                    } catch (Exception ex) {
                        LOG.trace("Error while creating URI from failover server: {}", redirect);
                    }
                }
            }
        }

        return alternates;
    }

    public Engine getProtonEngine() {
        return protonEngine;
    }

    public Connection getProtonConnection() {
        return protonConnection;
    }

    public ScheduledExecutorService getScheduler() {
        return this.serializer;
    }

    /**
     * Allows a resource to request that its parent resource schedule a future
     * cancellation of a request and return it a {@link Future} instance that
     * can be used to cancel the scheduled automatic failure of the request.
     *
     * @param request
     *      The request that should be marked as failed based on configuration.
     * @param timeout
     *      The time to wait before marking the request as failed.
     * @param error
     *      The error to use when failing the pending request.
     *
     * @return a {@link ScheduledFuture} that can be stored by the caller.
     */
    public ScheduledFuture<?> scheduleRequestTimeout(final AsyncResult request, long timeout, final ProviderException error) {
        if (timeout != JmsConnectionInfo.INFINITE) {
            return serializer.schedule(() -> {
                request.onFailure(error);
            }, timeout, TimeUnit.MILLISECONDS);
        }

        return null;
    }

    /**
     * Allows a resource to request that its parent resource schedule a future
     * cancellation of a request and return it a {@link Future} instance that
     * can be used to cancel the scheduled automatic failure of the request.
     *
     * @param request
     *      The request that should be marked as failed based on configuration.
     * @param timeout
     *      The time to wait before marking the request as failed.
     * @param builder
     *      An AmqpExceptionBuilder to use when creating a timed out exception.
     *
     * @return a {@link ScheduledFuture} that can be stored by the caller.
     */
    public ScheduledFuture<?> scheduleRequestTimeout(final AsyncResult request, long timeout, final AmqpExceptionBuilder builder) {
        if (timeout != JmsConnectionInfo.INFINITE) {
            return serializer.schedule(() -> {
                request.onFailure(builder.createException());
            }, timeout, TimeUnit.MILLISECONDS);
        }

        return null;
    }

    //----- Internal implementation ------------------------------------------//

    private void checkClosedOrFailed() throws ProviderException {
        if (closed.get()) {
            throw new ProviderClosedException("This Provider is already closed");
        }

        if (failureCause != null) {
            throw new ProviderFailedException("The Provider has failed", failureCause);
        }
    }

    private void checkConnected() throws ProviderException {
        if (serializer == null) {
            throw new ProviderClosedException("Transport has not been properly connected.");
        }
    }

    private SSLContext configureTransport(JmsConnectionInfo connectionInfo) {
        ThreadFactory transportThreadFactory = new QpidJMSThreadFactory(
                "AmqpProvider :(" + PROVIDER_SEQUENCE.incrementAndGet() + "):[" +
                remoteURI.getScheme() + "://" + remoteURI.getHost() + ":" + remoteURI.getPort() + "]", true);

        transport.setThreadFactory(transportThreadFactory);
        transport.setTransportListener(AmqpProvider.this);
        transport.setMaxFrameSize(maxFrameSize);

        final SSLContext sslContextOverride;
        if (connectionInfo.getExtensionMap().containsKey(JmsConnectionExtensions.SSL_CONTEXT)) {
            sslContextOverride =
                (SSLContext) connectionInfo.getExtensionMap().get(
                    JmsConnectionExtensions.SSL_CONTEXT).apply(connectionInfo.getConnection(), transport.getRemoteLocation());
        } else {
            sslContextOverride = null;
        }

        if (connectionInfo.getExtensionMap().containsKey(JmsConnectionExtensions.PROXY_HANDLER_SUPPLIER)) {
            Supplier<?> proxyHandlerSupplier = (Supplier<?>) connectionInfo.getExtensionMap().get(
                    JmsConnectionExtensions.PROXY_HANDLER_SUPPLIER).apply(connectionInfo.getConnection(), transport.getRemoteLocation());
            if (proxyHandlerSupplier != null) {
                transport.getTransportOptions().setProxyHandlerSupplier(proxyHandlerSupplier);
            }
        }

        if (connectionInfo.getExtensionMap().containsKey(JmsConnectionExtensions.HTTP_HEADERS_OVERRIDE)) {
            @SuppressWarnings({ "unchecked" })
            Map<String, String> headers = (Map<String, String>)
                connectionInfo.getExtensionMap().get(
                    JmsConnectionExtensions.HTTP_HEADERS_OVERRIDE).apply(connectionInfo.getConnection(), transport.getRemoteLocation());
            if (headers != null) {
                transport.getTransportOptions().getHttpHeaders().putAll(headers);
            }
        }

        return sslContextOverride;
    }

    private Engine configureEngineSaslSupport() {
        if (saslLayer) {
            SaslMechanismSelector mechSelector = new SaslMechanismFinder(saslMechanisms);
            SaslAuthenticator authenticator = new SaslAuthenticator(mechSelector, new SaslCredentialsProvider() {

                private final Map<String, Object> configOptions = createSaslOptions();

                @Override
                public String vhost() {
                    String hostname = getVhost() != null ? getVhost() : remoteURI.getHost();
                    if (hostname.isEmpty()) {
                        hostname = null;
                    }

                    return hostname;
                }

                @Override
                public String username() {
                    if (connectionInfo.getExtensionMap().containsKey(JmsConnectionExtensions.USERNAME_OVERRIDE)) {
                        return (String) connectionInfo.getExtensionMap().get(
                            JmsConnectionExtensions.USERNAME_OVERRIDE).apply(connectionInfo.getConnection(), transport.getRemoteLocation());
                    } else {
                        return connectionInfo.getUsername();
                    }
                }

                @Override
                public String password() {
                    if (connectionInfo.getExtensionMap().containsKey(JmsConnectionExtensions.PASSWORD_OVERRIDE)) {
                        return (String) connectionInfo.getExtensionMap().get(
                            JmsConnectionExtensions.PASSWORD_OVERRIDE).apply(connectionInfo.getConnection(), transport.getRemoteLocation());
                    } else {
                        return connectionInfo.getPassword();
                    }
                }

                @Override
                public Principal localPrincipal() {
                    return transport.getLocalPrincipal();
                }

                @Override
                public Map<String, Object> options() {
                    return configOptions;
                }
            });

            protonEngine.saslDriver().client().setListener(authenticator);
        }

        return protonEngine;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Map<String, Object> createSaslOptions() throws SaslSecurityRuntimeException {
        try {
            Map saslOptions =
                PropertyUtil.filterProperties(PropertyUtil.parseQuery(getRemoteURI()),"sasl.options.");

            if (!saslOptions.containsKey("serverName")) {
                saslOptions.put("serverName", remoteURI.getHost());
            }

            return Collections.unmodifiableMap(saslOptions);
        } catch (Exception ex) {
            throw new SaslSecurityRuntimeException(
                "Failed to create sasl options from URI configuration: reason: " + ex.toString(), ex);
        }
    }

    // TODO: Use auto tick feature in engine
    private final class IdleTimeoutCheck implements Runnable {
        @Override
        public void run() {
            boolean checkScheduled = false;

            if (connection.isOpen()) {
                // Using nano time since it is not related to the wall clock, which may change
                long now = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
                long deadline = protonEngine.tick(now);

                if (protonEngine.isShutdown()) {
                    LOG.info("IdleTimeoutCheck closed the transport due to the peer exceeding our requested idle-timeout.");
                } else {
                    if (deadline != 0) {
                        long delay = deadline - now;
                        checkScheduled = true;
                        LOG.trace("IdleTimeoutCheck rescheduling with delay: {}", delay);
                        nextIdleTimeoutCheck = serializer.schedule(this, delay, TimeUnit.MILLISECONDS);
                    }
                }
            } else {
                LOG.trace("IdleTimeoutCheck skipping check, connection is not active.");
            }

            if (!checkScheduled) {
                nextIdleTimeoutCheck = null;
                LOG.trace("IdleTimeoutCheck exiting");
            }
        }
    }
}
