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
package org.apache.qpid.jms.transports.netty;

import static org.apache.qpid.jms.transports.netty.NettyEventLoopGroupFactory.sharedGroup;
import static org.apache.qpid.jms.transports.netty.NettyEventLoopGroupFactory.unsharedGroup;

import java.io.IOException;
import java.net.URI;
import java.security.Principal;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;

import org.apache.qpid.jms.transports.Transport;
import org.apache.qpid.jms.transports.TransportListener;
import org.apache.qpid.jms.transports.TransportOptions;
import org.apache.qpid.jms.transports.TransportSupport;
import org.apache.qpid.jms.util.IOExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.contrib.handler.proxy.ProxyHandler;
import io.netty5.bootstrap.Bootstrap;
import io.netty5.buffer.api.Buffer;
import io.netty5.channel.Channel;
import io.netty5.channel.ChannelHandler;
import io.netty5.channel.ChannelHandlerContext;
import io.netty5.channel.ChannelInitializer;
import io.netty5.channel.ChannelOption;
import io.netty5.channel.ChannelPipeline;
import io.netty5.channel.EventLoop;
import io.netty5.channel.FixedRecvBufferAllocator;
import io.netty5.channel.SimpleChannelInboundHandler;
import io.netty5.handler.logging.LoggingHandler;
import io.netty5.handler.ssl.SslHandler;
import io.netty5.resolver.NoopAddressResolverGroup;
import io.netty5.util.concurrent.Future;
import io.netty5.util.concurrent.FutureListener;

/**
 * TCP based transport that uses Netty as the underlying IO layer.
 */
public class NettyTcpTransport implements Transport {

    private static final Logger LOG = LoggerFactory.getLogger(NettyTcpTransport.class);

    public static final int DEFAULT_MAX_FRAME_SIZE = 65535;

    protected EventLoopGroupRef groupRef;
    protected Channel channel;
    protected TransportListener listener;
    protected ThreadFactory ioThreadfactory;
    protected int maxFrameSize = DEFAULT_MAX_FRAME_SIZE;

    private final boolean secure;
    private final TransportOptions options;
    private final URI remote;
    private final AtomicBoolean connected = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CountDownLatch connectLatch = new CountDownLatch(1);
    private volatile IOException failureCause;

    /**
     * Create a new transport instance
     *
     * @param remoteLocation
     *        the URI that defines the remote resource to connect to.
     * @param options
     *        the transport options used to configure the socket connection.
     * @param secure
     * 		  should the transport enable an SSL layer.
     */
    public NettyTcpTransport(URI remoteLocation, TransportOptions options, boolean secure) {
        this(null, remoteLocation, options, secure);
    }

    /**
     * Create a new transport instance
     *
     * @param listener
     *        the TransportListener that will receive events from this Transport.
     * @param remoteLocation
     *        the URI that defines the remote resource to connect to.
     * @param options
     *        the transport options used to configure the socket connection.
     * @param secure
     * 		  should the transport enable an SSL layer.
     */
    public NettyTcpTransport(TransportListener listener, URI remoteLocation, TransportOptions options, boolean secure) {
        if (options == null) {
            throw new IllegalArgumentException("Transport Options cannot be null");
        }

        if (remoteLocation == null) {
            throw new IllegalArgumentException("Transport remote location cannot be null");
        }

        this.secure = secure;
        this.options = options;
        this.listener = listener;
        this.remote = remoteLocation;
    }

    @Override
    public EventLoop connect(final Runnable initRoutine, SSLContext sslContextOverride) throws IOException {
        if (closed.get()) {
            throw new IllegalStateException("Transport has already been closed");
        }

        if (listener == null) {
            throw new IllegalStateException("A transport listener must be set before connection attempts.");
        }

        TransportOptions transportOptions = getTransportOptions();

        EventLoopType eventLoopType = EventLoopType.valueOf(transportOptions);
        int sharedEventLoopThreads = transportOptions.getSharedEventLoopThreads();
        if (sharedEventLoopThreads > 0) {
            groupRef = sharedGroup(eventLoopType, sharedEventLoopThreads);
        } else {
            groupRef = unsharedGroup(eventLoopType, ioThreadfactory);
        }

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(groupRef.group());

        eventLoopType.createChannel(bootstrap);

        bootstrap.handler(new ChannelInitializer<Channel>() {
            @Override
            public void initChannel(Channel connectedChannel) throws Exception {
                if (initRoutine != null) {
                    try {
                        initRoutine.run();
                    } catch (Throwable initError) {
                        LOG.warn("Error during initialization of channel from provided initialization routine");
                        connectionFailed(connectedChannel, IOExceptionSupport.create(initError));
                        throw initError;
                    }
                }
                configureChannel(connectedChannel);
            }
        });

        configureNetty(bootstrap, transportOptions);
        transportOptions.setSslContextOverride(sslContextOverride);

        Future<Channel> future = bootstrap.connect(getRemoteHost(), getRemotePort());
        future.addListener(new FutureListener<Channel>() {

            @Override
            public void operationComplete(Future<? extends Channel> future) throws Exception {
                if (!future.isSuccess()) {
                    handleException(channel, IOExceptionSupport.create(future.cause()));
                }
            }
        });

        try {
            connectLatch.await();
        } catch (InterruptedException ex) {
            LOG.debug("Transport connection was interrupted.");
            Thread.interrupted();
            failureCause = IOExceptionSupport.create(ex);
        }

        if (failureCause != null) {
            // Close out any Netty resources now as they are no longer needed.
            if (channel != null) {
                channel.close().syncUninterruptibly();
                channel = null;
            }

            throw failureCause;
        } else {
            // Connected, allow any held async error to fire now and close the transport.
            channel.executor().execute(() -> {
                if (failureCause != null) {
                    channel.pipeline().fireExceptionCaught(failureCause);
                }
            });
        }
        // returning the channel's specific event loop: the overall event loop group may be multi-threaded
        return channel.executor();
    }

    @Override
    public boolean isConnected() {
        return connected.get();
    }

    @Override
    public boolean isSecure() {
        return secure;
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            connected.set(false);
            try {
                if (channel != null) {
                    channel.close().syncUninterruptibly();
                }
            } finally {
                if (groupRef != null) {
                    groupRef.close();
                }
            }
        }
    }

    @Override
    public Buffer allocateSendBuffer(int size) throws IOException {
        checkConnected();
        return channel.bufferAllocator().allocate(size);
    }

    @Override
    public void write(Buffer output) throws IOException {
        checkConnected(output);
        LOG.trace("Attempted write of buffer: {}", output);
        channel.write(output);
    }

    @Override
    public void writeAndFlush(Buffer output) throws IOException {
        checkConnected(output);
        LOG.trace("Attempted write and flush of buffer: {}", output);
        channel.writeAndFlush(output);
    }

    @Override
    public void flush() throws IOException {
        checkConnected();
        LOG.trace("Attempted flush of pending writes");
        channel.flush();
    }

    @Override
    public TransportListener getTransportListener() {
        return listener;
    }

    @Override
    public void setTransportListener(TransportListener listener) {
        this.listener = listener;
    }

    @Override
    public TransportOptions getTransportOptions() {
        return options;
    }

    @Override
    public URI getRemoteLocation() {
        return remote;
    }

    @Override
    public Principal getLocalPrincipal() {
        Principal result = null;

        if (isSecure()) {
            SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
            result = sslHandler.engine().getSession().getLocalPrincipal();
        }

        return result;
    }

    @Override
    public void setMaxFrameSize(int maxFrameSize) {
        if (connected.get()) {
            throw new IllegalStateException("Cannot change Max Frame Size while connected.");
        }

        this.maxFrameSize = maxFrameSize;
    }

    @Override
    public int getMaxFrameSize() {
        return maxFrameSize;
    }

    @Override
    public ThreadFactory getThreadFactory() {
        return ioThreadfactory;
    }

    @Override
    public void setThreadFactory(ThreadFactory factory) {
        if (isConnected() || channel != null) {
            throw new IllegalStateException("Cannot set IO ThreadFactory after Transport connect");
        }

        this.ioThreadfactory = factory;
    }

    //----- Internal implementation details, can be overridden as needed -----//

    protected String getRemoteHost() {
        return remote.getHost();
    }

    protected int getRemotePort() {
        if (remote.getPort() != -1) {
            return remote.getPort();
        } else {
            return isSecure() ? getTransportOptions().getDefaultSslPort() : getTransportOptions().getDefaultTcpPort();
        }
    }

    protected void addAdditionalHandlers(ChannelPipeline pipeline) {

    }

    protected ChannelHandler createChannelHandler() {
        return new NettyTcpTransportHandler();
    }

    //----- Event Handlers which can be overridden in subclasses -------------//

    protected void handleConnected(Channel channel) throws Exception {
        LOG.trace("Channel has become active! Channel is {}", channel);
        connectionEstablished(channel);
    }

    protected void handleChannelInactive(Channel channel) throws Exception {
        LOG.trace("Channel has gone inactive! Channel is {}", channel);
        if (connected.compareAndSet(true, false) && !closed.get()) {
            LOG.trace("Firing onTransportClosed listener");
            if (channel.executor().inEventLoop()) {
                listener.onTransportClosed();
            } else {
                channel.executor().execute(() -> {
                    listener.onTransportClosed();
                });
            }
        } else if (!closed.get()) {
            if (failureCause == null) {
                failureCause = new IOException("Connection failed");
            }
            connectionFailed(channel, failureCause);
        }
    }

    protected void handleException(Channel channel, Throwable cause) {
        if (channel == null) {
            LOG.trace("Exception on connect! error is {}", cause.getMessage());
        } else {
            LOG.trace("Exception on channel! Channel is {}", channel);
        }

        if (connected.compareAndSet(true, false) && !closed.get()) {
            LOG.trace("Firing onTransportError listener");
            if (channel.executor().inEventLoop()) {
                if (failureCause != null) {
                    listener.onTransportError(failureCause);
                } else {
                    listener.onTransportError(cause);
                }
            } else {
                channel.executor().execute(() -> {
                    if (failureCause != null) {
                        listener.onTransportError(failureCause);
                    } else {
                        listener.onTransportError(cause);
                    }
                });
            }
        } else {
            // Hold the first failure for later dispatch if connect succeeds.
            // This will then trigger disconnect using the first error reported.
            if (failureCause == null) {
                LOG.trace("Holding error until connect succeeds: {}", cause.getMessage());
                failureCause = IOExceptionSupport.create(cause);
            }

            connectionFailed(channel, failureCause);
        }
    }

    //----- State change handlers and checks ---------------------------------//

    protected final void checkConnected() throws IOException {
        if (!connected.get() || !channel.isActive()) {
            throw new IOException("Cannot send to a non-connected transport.");
        }
    }

    private void checkConnected(Buffer output) throws IOException {
        if (!connected.get() || !channel.isActive()) {
            output.close();
            throw new IOException("Cannot send to a non-connected transport.");
        }
    }

    /*
     * Called when the transport has successfully connected and is ready for use.
     */
    private void connectionEstablished(Channel connectedChannel) {
        channel = connectedChannel;
        connected.set(true);
        connectLatch.countDown();
    }

    /*
     * Called when the transport connection failed and an error should be returned.
     */
    private void connectionFailed(Channel failedChannel, IOException cause) {
        failureCause = cause;
        channel = failedChannel;
        connected.set(false);
        connectLatch.countDown();
    }

    private void configureNetty(Bootstrap bootstrap, TransportOptions options) {
        bootstrap.option(ChannelOption.TCP_NODELAY, options.isTcpNoDelay());
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, options.getConnectTimeout());
        bootstrap.option(ChannelOption.SO_KEEPALIVE, options.isTcpKeepAlive());
        bootstrap.option(ChannelOption.SO_LINGER, options.getSoLinger());

        if (options.getSendBufferSize() != -1) {
            bootstrap.option(ChannelOption.SO_SNDBUF, options.getSendBufferSize());
        }

        if (options.getReceiveBufferSize() != -1) {
            bootstrap.option(ChannelOption.SO_RCVBUF, options.getReceiveBufferSize());
            bootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvBufferAllocator(options.getReceiveBufferSize()));
        }

        if (options.getTrafficClass() != -1) {
            bootstrap.option(ChannelOption.IP_TOS, options.getTrafficClass());
        }

        if (options.getLocalAddress() != null || options.getLocalPort() != 0) {
            if(options.getLocalAddress() != null) {
                bootstrap.localAddress(options.getLocalAddress(), options.getLocalPort());
            } else {
                bootstrap.localAddress(options.getLocalPort());
            }
        }
        if (options.getProxyHandlerSupplier() != null) {
            // in case we have a proxy we do not want to resolve the address by ourselves but leave this to the proxy
            bootstrap.resolver(NoopAddressResolverGroup.INSTANCE);
        }
    }

    private void configureChannel(final Channel channel) throws Exception {
        if (options.getProxyHandlerSupplier() != null) {
            Supplier<ProxyHandler> proxyHandlerSupplier = options.getProxyHandlerSupplier();

            ProxyHandler proxyHandler = proxyHandlerSupplier.get();
            Objects.requireNonNull(proxyHandler, "No proxy handler was returned by the supplier");

            channel.pipeline().addFirst(proxyHandler);
        }
        if (isSecure()) {
            final SslHandler sslHandler;
            try {
                sslHandler = TransportSupport.createSslHandler(channel.bufferAllocator(), getRemoteLocation(), getTransportOptions());
            } catch (Exception ex) {
                throw IOExceptionSupport.create(ex);
            }

            channel.pipeline().addLast("ssl", sslHandler);
        }

        if (getTransportOptions().isTraceBytes()) {
            channel.pipeline().addLast("logger", new LoggingHandler(getClass()));
        }

        addAdditionalHandlers(channel.pipeline());

        channel.pipeline().addLast(createChannelHandler());
    }

    //----- Default implementation of Netty handler --------------------------//

    protected abstract class NettyDefaultHandler<E> extends SimpleChannelInboundHandler<E> {

        @Override
        public void channelRegistered(ChannelHandlerContext context) throws Exception {
            channel = context.channel();
        }

        @Override
        public void channelActive(ChannelHandlerContext context) throws Exception {
            // In the Secure case we need to let the handshake complete before we
            // trigger the connected event.
            if (!isSecure()) {
                handleConnected(context.channel());
            } else {
                SslHandler sslHandler = context.pipeline().get(SslHandler.class);
                sslHandler.handshakeFuture().addListener(new FutureListener<Channel>() {

                    @Override
                    public void operationComplete(Future<? extends Channel> future) throws Exception {
                        if (future.isSuccess()) {
                            LOG.trace("SSL Handshake has completed: {}", channel);
                            handleConnected(channel);
                        } else {
                            LOG.trace("SSL Handshake has failed: {}", channel);
                            handleException(channel, future.cause());
                        }
                    }
                });
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext context) throws Exception {
            handleChannelInactive(context.channel());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, Throwable cause) throws Exception {
            handleException(context.channel(), cause);
        }
    }

    //----- Handle binary data over socket connections -----------------------//

    protected class NettyTcpTransportHandler extends NettyDefaultHandler<Buffer> {

        @Override
        protected void messageReceived(ChannelHandlerContext ctx, Buffer buffer) throws Exception {
            LOG.trace("New incoming data read: {}", buffer);
            // Avoid all doubts to the contrary
            if (channel.executor().inEventLoop()) {
                listener.onData(buffer);
            } else {
                channel.executor().execute(() -> {
                    listener.onData(buffer);
                });
            }
        }
    }
}
