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
package org.apache.qpid.jms.transports.plugins;

import org.apache.qpid.jms.transports.Transport;
import org.apache.qpid.jms.transports.TransportOptions;
import org.apache.qpid.jms.transports.TransportPlugin;
import org.apache.qpid.jms.transports.TransportPluginFactory;
import org.apache.qpid.jms.transports.netty.NettyTcpTransport;
import org.apache.qpid.jms.transports.netty.NettyTcpTransportPlugin;
import org.apache.qpid.jms.transports.netty.NettyWsTransport;

import io.netty.handler.ssl.SslHandler;

/**
 * Mock plugin implementing {@link NettyTcpTransportPlugin}
 */
public class MockNettyTcpTransportPlugin extends TransportPluginFactory implements NettyTcpTransportPlugin {

    @Override
    public boolean isApplicable(Transport target) {
        return target instanceof NettyTcpTransport && !(target instanceof NettyWsTransport);
    }

    @Override
    public String getConfigurationPrefix() {
        return "mock";
    }

    @Override
    public void configureTransportOptions(Transport transport, TransportOptions options) {
    }

    @Override
    public SslHandler configureSslHandler(NettyTcpTransport transport, SslHandler sslHandler) {
        return sslHandler;
    }

    @Override
    public TransportPlugin createPlugin() throws Exception {
        return new MockNettyTcpTransportPlugin();
    }
}
