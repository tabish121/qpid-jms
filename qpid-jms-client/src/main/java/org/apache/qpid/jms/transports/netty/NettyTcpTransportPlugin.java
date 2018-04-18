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

import org.apache.qpid.jms.transports.TransportPlugin;

import io.netty.handler.ssl.SslHandler;

/**
 * A Transport plugin that is aware of the Qpid JMS NettyTcpTransport
 * and implements hooks specific to that transport.
 */
public interface NettyTcpTransportPlugin extends TransportPlugin {

    /**
     * Called before a connection attempt is made to allow a plugin to customize the
     * Netty SslHandler that will be used by the given {@link NettyTcpTransport}.
     *
     * @param transport
     * 		The {@link NettyTcpTransport} that is going to perform a connect.
     * @param sslHandler
     * 		Thhe Netty {@link SslHandler} that will be used to secure the TCP connection.
     *
     * @return the configured {@link SslHandler} that the {@link NettyTcpTransport} will use.
     */
    SslHandler configureSslHandler(NettyTcpTransport transport, SslHandler sslHandler);

}
