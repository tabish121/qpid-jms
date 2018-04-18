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

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;

/**
 * A Transport plugin that is aware of the Qpid JMS NettyWsTransport
 * and implements hooks specific to that transport.
 */
public interface NettyWsTransportPlugin extends NettyTcpTransportPlugin {

    /**
     * Called before a connection attempt is made to allow a plugin to customize the
     * Netty DefaultHttpHeaders that will be used by the given {@link NettyWsTransport}.
     *
     * @param transport
     * 		The Transport that is going to perform a connect.
     * @param headers
     * 		The Netty {@link HttpHeaders} that will be used to establish the WS connection.
     *
     * @return the configured HttpHeaders that the {@link NettyWsTransport} will use.
     */
    HttpHeaders configurHttpHeaders(NettyWsTransport transport, HttpHeaders headers);

    /**
     * Called before a connection attempt is made to allow a plugin to customize the
     * Netty {@link WebSocketClientHandshaker} that will be used by the given {@link NettyWsTransport}.
     *
     * @param transport
     * 		The Transport that is going to perform a connect.
     * @param handshaker
     * 		The Netty {@link WebSocketClientHandshaker} that will be used to establish the WS connection.
     *
     * @return the configured handshaker that the {@link NettyWsTransport} will use.
     */
    WebSocketClientHandshaker configureHandshaker(NettyWsTransport transport, WebSocketClientHandshaker handshaker);

}
