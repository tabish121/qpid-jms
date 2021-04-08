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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.exceptions.ProviderConnectionRedirectedException;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;
import org.junit.Test;
import org.mockito.Mockito;

public class AmqpSupportTest {

    @Test
    public void testCreateRedirectionException() throws URISyntaxException {
        AmqpProvider mockProvider = Mockito.mock(AmqpProvider.class);
        Mockito.when(mockProvider.getRemoteURI()).thenReturn(new URI("amqp://localhost:5672"));

        Map<Symbol, Object> info = new HashMap<>();
        info.put(AmqpSupport.PORT, "5672");
        info.put(AmqpSupport.OPEN_HOSTNAME, "localhost.localdomain");
        info.put(AmqpSupport.NETWORK_HOST, "localhost");
        info.put(AmqpSupport.SCHEME, "amqp");
        info.put(AmqpSupport.PATH, "/websocket");

        Symbol error = AmqpError.INTERNAL_ERROR;
        String message = "Failed to connect";

        ErrorCondition condition = new ErrorCondition(error, message, info);

        Exception result = AmqpSupport.createRedirectException(mockProvider, error, message, condition);

        assertNotNull(result);
        assertTrue(result instanceof ProviderConnectionRedirectedException);

        ProviderConnectionRedirectedException pre = (ProviderConnectionRedirectedException) result;

        URI redirection = pre.getRedirectionURI();

        assertEquals(5672, redirection.getPort());
        assertTrue("localhost.localdomain", redirection.getQuery().contains("amqp.vhost=localhost.localdomain"));
        assertEquals("localhost", redirection.getHost());
        assertEquals("amqp", redirection.getScheme());
        assertEquals("/websocket", redirection.getPath());
    }

    @Test
    public void testCreateRedirectionExceptionWithNoRedirectInfo() throws URISyntaxException {
        AmqpProvider mockProvider = Mockito.mock(AmqpProvider.class);
        Mockito.when(mockProvider.getRemoteURI()).thenReturn(new URI("amqp://localhost:5672"));

        Symbol error = AmqpError.INTERNAL_ERROR;
        String message = "Failed to connect";

        ErrorCondition condition = new ErrorCondition(error, message);

        Exception result = AmqpSupport.createRedirectException(mockProvider, error, message, condition);

        assertNotNull(result);
        assertFalse(result instanceof ProviderConnectionRedirectedException);
        assertTrue(result instanceof ProviderException);
    }

    @Test
    public void testCreateRedirectionExceptionWithNoNetworkHost() throws URISyntaxException {
        AmqpProvider mockProvider = Mockito.mock(AmqpProvider.class);
        Mockito.when(mockProvider.getRemoteURI()).thenReturn(new URI("amqp://localhost:5672"));


        Map<Symbol, Object> info = new HashMap<>();
        info.put(AmqpSupport.PORT, "5672");
        info.put(AmqpSupport.OPEN_HOSTNAME, "localhost");
        info.put(AmqpSupport.SCHEME, "amqp");
        info.put(AmqpSupport.PATH, "websocket");

        Symbol error = AmqpError.INTERNAL_ERROR;
        String message = "Failed to connect";

        ErrorCondition condition = new ErrorCondition(error, message, info);

        Exception result = AmqpSupport.createRedirectException(mockProvider, error, message, condition);

        assertNotNull(result);
        assertFalse(result instanceof ProviderConnectionRedirectedException);
        assertTrue(result instanceof ProviderException);
    }

    @Test
    public void testCreateRedirectionExceptionWithEmptyNetworkHost() throws URISyntaxException {
        AmqpProvider mockProvider = Mockito.mock(AmqpProvider.class);
        Mockito.when(mockProvider.getRemoteURI()).thenReturn(new URI("amqp://localhost:5672"));

        Map<Symbol, Object> info = new HashMap<>();
        info.put(AmqpSupport.PORT, "5672");
        info.put(AmqpSupport.NETWORK_HOST, "");
        info.put(AmqpSupport.OPEN_HOSTNAME, "localhost");
        info.put(AmqpSupport.SCHEME, "amqp");
        info.put(AmqpSupport.PATH, "websocket");

        Symbol error = AmqpError.INTERNAL_ERROR;
        String message = "Failed to connect";

        ErrorCondition condition = new ErrorCondition(error, message, info);

        Exception result = AmqpSupport.createRedirectException(mockProvider, error, message, condition);

        assertNotNull(result);
        assertFalse(result instanceof ProviderConnectionRedirectedException);
        assertTrue(result instanceof ProviderException);
    }

    @Test
    public void testCreateRedirectionExceptionWithInvalidPort() throws URISyntaxException {
        AmqpProvider mockProvider = Mockito.mock(AmqpProvider.class);
        Mockito.when(mockProvider.getRemoteURI()).thenReturn(new URI("amqp://localhost:5672"));

        Map<Symbol, Object> info = new HashMap<>();
        info.put(AmqpSupport.PORT, "L5672");
        info.put(AmqpSupport.OPEN_HOSTNAME, "localhost");
        info.put(AmqpSupport.NETWORK_HOST, "localhost");
        info.put(AmqpSupport.SCHEME, "amqp");
        info.put(AmqpSupport.PATH, "websocket");

        Symbol error = AmqpError.INTERNAL_ERROR;
        String message = "Failed to connect";

        ErrorCondition condition = new ErrorCondition(error, message, info);

        Exception result = AmqpSupport.createRedirectException(mockProvider, error, message, condition);

        assertNotNull(result);
        assertFalse(result instanceof ProviderConnectionRedirectedException);
        assertTrue(result instanceof ProviderException);
    }
}
