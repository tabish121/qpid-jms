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

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.qpid.jms.transports.TransportOptions;
import org.apache.qpid.jms.transports.TransportSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty5.bootstrap.ServerBootstrap;
import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.DefaultBufferAllocators;
import io.netty5.channel.Channel;
import io.netty5.channel.ChannelHandler;
import io.netty5.channel.ChannelHandlerAdapter;
import io.netty5.channel.ChannelHandlerContext;
import io.netty5.channel.ChannelInitializer;
import io.netty5.channel.ChannelOption;
import io.netty5.channel.EventLoopGroup;
import io.netty5.channel.MultithreadEventLoopGroup;
import io.netty5.channel.nio.NioHandler;
import io.netty5.channel.socket.nio.NioServerSocketChannel;
import io.netty5.handler.codec.http.DefaultFullHttpResponse;
import io.netty5.handler.codec.http.FullHttpRequest;
import io.netty5.handler.codec.http.FullHttpResponse;
import io.netty5.handler.codec.http.HttpObjectAggregator;
import io.netty5.handler.codec.http.HttpResponseStatus;
import io.netty5.handler.codec.http.HttpServerCodec;
import io.netty5.handler.codec.http.HttpUtil;
import io.netty5.handler.codec.http.HttpVersion;
import io.netty5.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty5.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty5.handler.codec.http.websocketx.WebSocketFrame;
import io.netty5.handler.codec.http.websocketx.WebSocketServerHandshakeCompletionEvent;
import io.netty5.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty5.handler.codec.http.websocketx.extensions.compression.WebSocketServerCompressionHandler;
import io.netty5.handler.logging.LogLevel;
import io.netty5.handler.logging.LoggingHandler;
import io.netty5.handler.ssl.SslHandler;
import io.netty5.util.concurrent.Future;
import io.netty5.util.concurrent.FutureListener;

/**
 * Base Server implementation used to create Netty based server implementations for
 * unit testing aspects of the client code.
 */
public abstract class NettyServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(NettyServer.class);

    static final String WEBSOCKET_PATH = "/";
    static final int SERVER_CHOOSES_PORT = 0;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private final TransportOptions options;
    private final boolean secure;
    private int serverPort = SERVER_CHOOSES_PORT;
    private final boolean needClientAuth;
    private final boolean webSocketServer;
    private int maxFrameSize = NettyTcpTransport.DEFAULT_MAX_FRAME_SIZE;
    private String webSocketPath = WEBSOCKET_PATH;
    private volatile boolean fragmentWrites;
    private volatile SslHandler sslHandler;
    private volatile WebSocketServerHandshakeCompletionEvent handshakeComplete;
    private final CountDownLatch handshakeCompletion = new CountDownLatch(1);
    private final AtomicInteger channelActiveCount = new AtomicInteger();

    private final AtomicBoolean started = new AtomicBoolean();

    public NettyServer(TransportOptions options, boolean secure) {
        this(options, secure, false);
    }

    public NettyServer(TransportOptions options, boolean secure, boolean needClientAuth) {
        this(options, secure, needClientAuth, false);
    }

    public NettyServer(TransportOptions options, boolean secure, boolean needClientAuth, boolean webSocketServer) {
        this.secure = secure;
        this.options = options;
        this.needClientAuth = needClientAuth;
        this.webSocketServer = webSocketServer;
    }

    public boolean isSecureServer() {
        return secure;
    }

    public boolean isWebSocketServer() {
        return webSocketServer;
    }

    public String getWebSocketPath() {
        return webSocketPath;
    }

    public void setWebSocketPath(String webSocketPath) {
        this.webSocketPath = webSocketPath;
    }

    public int getMaxFrameSize() {
        return maxFrameSize;
    }

    public void setMaxFrameSize(int maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    public void setFragmentWrites(boolean fragmentWrites) {
        if(!webSocketServer) {
            throw new IllegalStateException("Only applicable to WebSocket servers");
        }

        this.fragmentWrites = fragmentWrites;
    }

    public boolean isFragmentWrites() {
        return fragmentWrites;
    }

    public boolean awaitHandshakeCompletion(long delayMs) throws InterruptedException {
        return handshakeCompletion.await(delayMs, TimeUnit.MILLISECONDS);
    }

    public WebSocketServerHandshakeCompletionEvent getHandshakeComplete() {
        return handshakeComplete;
    }

    public URI getConnectionURI() throws Exception {
        if (!started.get()) {
            throw new IllegalStateException("Cannot get URI of non-started server");
        }

        int port = getServerPort();

        String scheme;
        String path;

        if (isWebSocketServer()) {
            if (isSecureServer()) {
                scheme = "amqpwss";
            } else {
                scheme = "amqpws";
            }
        } else {
            if (isSecureServer()) {
                scheme = "amqps";
            } else {
                scheme = "amqp";
            }
        }

        if (isWebSocketServer()) {
            path = getWebSocketPath();
        } else {
            path = null;
        }

        return new URI(scheme, null, "localhost", port, path, null, null);
    }

    public void start() throws Exception {
        start(serverPort);
    }

    public void start(int listenOn) throws Exception {
        if (started.compareAndSet(false, true)) {

            // Basic server configuration with NIO only options.
            bossGroup = new MultithreadEventLoopGroup(1, NioHandler.newFactory());
            workerGroup = new MultithreadEventLoopGroup(NioHandler.newFactory());

            ServerBootstrap server = new ServerBootstrap();
            server.group(bossGroup, workerGroup);
            server.channel(NioServerSocketChannel.class);
            server.option(ChannelOption.SO_BACKLOG, 100);
            server.handler(new LoggingHandler(LogLevel.INFO));
            server.childHandler(new ChannelInitializer<Channel>() {

                @SuppressWarnings("rawtypes")
                @Override
                public void initChannel(Channel ch) throws Exception {
                    if (isSecureServer()) {
                        SSLContext context = TransportSupport.createJdkSslContext(options);
                        SSLEngine engine = TransportSupport.createJdkSslEngine(null, context, options);
                        engine.setUseClientMode(false);
                        engine.setNeedClientAuth(needClientAuth);
                        sslHandler = new SslHandler(engine);
                        ch.pipeline().addLast(sslHandler);
                    }

                    if (webSocketServer) {
                        ch.pipeline().addLast(new HttpServerCodec());
                        ch.pipeline().addLast(new HttpObjectAggregator(65536));
                        ch.pipeline().addLast(new WebSocketServerCompressionHandler());
                        ch.pipeline().addLast(new WebSocketServerProtocolHandler(getWebSocketPath(), "amqp", true, maxFrameSize));
                    }

                    ch.pipeline().addLast(new NettyServerOutboundHandler());
                    ch.pipeline().addLast(new NettyServerInboundHandler());
                    ch.pipeline().addLast(getServerHandler());
                }
            });

            // Start the server using specified port.  If value is zero the server
            // will select a free port and so we update the server port value after
            // in order to reflect the correct value.
            serverChannel = server.bind(listenOn).asStage().sync().get();
            serverPort = ((InetSocketAddress) serverChannel.localAddress()).getPort();
        }
    }

    protected abstract ChannelHandler getServerHandler();

    public void stop() throws InterruptedException {
        if (started.compareAndSet(true, false)) {
            try {
                LOG.info("Syncing channel close");
                serverChannel.close().asStage().sync();
            } catch (InterruptedException e) {
            }

            // Shut down all event loops to terminate all threads.
            int timeout = 100;
            LOG.trace("Shutting down boss group");
            bossGroup.shutdownGracefully(0, timeout, TimeUnit.MILLISECONDS).asStage().await(timeout, TimeUnit.MILLISECONDS);
            LOG.trace("Boss group shut down");

            LOG.trace("Shutting down worker group");
            workerGroup.shutdownGracefully(0, timeout, TimeUnit.MILLISECONDS).asStage().await(timeout, TimeUnit.MILLISECONDS);
            LOG.trace("Worker group shut down");
        }
    }

    @Override
    public void close() throws InterruptedException {
        stop();
    }

    public int getServerPort() {
        if (!started.get()) {
            throw new IllegalStateException("Cannot get server port of non-started server");
        }

        return serverPort;
    }

    private class NettyServerOutboundHandler extends ChannelHandlerAdapter  {

        @Override
        public Future<Void> write(ChannelHandlerContext ctx, Object msg) {
            LOG.trace("NettyServerHandler: Channel write: {}", msg);
            if (isWebSocketServer() && msg instanceof Buffer) {
                if (isFragmentWrites()) {
                    Buffer orig = (Buffer) msg;
                    int origIndex = orig.readerOffset();
                    int split = orig.readableBytes()/2;

                    Buffer part1 = orig.copy(origIndex, split);
                    LOG.trace("NettyServerHandler: Part1: {}", part1);
                    orig.readerOffset(origIndex + split);
                    LOG.trace("NettyServerHandler: Part2: {}", orig);

                    BinaryWebSocketFrame frame1 = new BinaryWebSocketFrame(false, 0, part1);
                    ctx.writeAndFlush(frame1);
                    ContinuationWebSocketFrame frame2 = new ContinuationWebSocketFrame(true, 0, orig);
                    return ctx.write(frame2);
                } else {
                    BinaryWebSocketFrame frame = new BinaryWebSocketFrame((Buffer) msg);
                    return ctx.write(frame);
                }
            } else {
                return ctx.write(msg);
            }
        }
    }

    private class NettyServerInboundHandler extends ChannelHandlerAdapter  {

        @Override
        public void channelInboundEvent(ChannelHandlerContext ctx, Object event) throws Exception {
            if (event instanceof WebSocketServerHandshakeCompletionEvent) {
                handshakeComplete = (WebSocketServerHandshakeCompletionEvent) event;
                handshakeCompletion.countDown();
            }
        }

        @Override
        public void channelActive(final ChannelHandlerContext ctx) {
            LOG.info("NettyServerHandler -> New active channel: {}", ctx.channel());
            SslHandler handler = ctx.pipeline().get(SslHandler.class);
            if (handler != null) {
                handler.handshakeFuture().addListener(new FutureListener<Channel>() {
                    @Override
                    public void operationComplete(Future<? extends Channel> future) throws Exception {
                        LOG.info("Server -> SSL handshake completed. Succeeded: {}", future.isSuccess());
                        if (!future.isSuccess()) {
                            ctx.close();
                        }
                    }
                });
            }

            channelActiveCount.incrementAndGet();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            LOG.info("NettyServerHandler: channel has gone inactive: {}", ctx.channel());
            ctx.close();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            LOG.trace("NettyServerHandler: Channel read: {}", msg);
            if (msg instanceof WebSocketFrame) {
                WebSocketFrame frame = (WebSocketFrame) msg;
                ctx.fireChannelRead(frame.binaryData());
            } else if (msg instanceof FullHttpRequest) {
                // Reject anything not on the WebSocket path
                FullHttpRequest request = (FullHttpRequest) msg;
                sendHttpResponse(ctx, request,
                    new DefaultFullHttpResponse(
                        HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST, DefaultBufferAllocators.onHeapAllocator().allocate(0)));
            } else {
                // Forward anything else along to the next handler.
                ctx.fireChannelRead(msg);
            }
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void channelExceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOG.info("NettyServerHandler: NettyServerHandlerException caught on channel: {}", ctx.channel());
            // Close the connection when an exception is raised.
            cause.printStackTrace();
            ctx.close();
        }
    }

    private static void sendHttpResponse(ChannelHandlerContext ctx, FullHttpRequest request, FullHttpResponse response) {
        // Generate an error page if response getStatus code is not OK (200).
        if (response.status().code() != 200) {
            try (Buffer buf = BufferAllocator.onHeapUnpooled().copyOf(response.status().toString().getBytes(StandardCharsets.UTF_8))) {
                response.payload().writeBytes(buf);
                HttpUtil.setContentLength(response, response.payload().readableBytes());
            }
        }

        // Send the response and close the connection if necessary.
        Future<Void> f = ctx.channel().writeAndFlush(response);
        if (!HttpUtil.isKeepAlive(request) || response.status().code() != 200) {
            f.addListener((channel) -> ctx.channel().close());
        }
    }

    protected SslHandler getSslHandler() {
        return sslHandler;
    }

    public int getChannelActiveCount() {
        return channelActiveCount.get();
    }
}
