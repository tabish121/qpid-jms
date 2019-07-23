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
package org.apache.qpid.jms.provider;

import java.net.URI;
import java.util.List;

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsMessageFactory;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.meta.JmsSessionId;
import org.apache.qpid.jms.meta.JmsTransactionInfo;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;

/**
 * Allows one {@link Provider} instance to wrap around another and provide some additional
 * features beyond the normal {@link Provider} interface.
 *
 * This wrapper is meant primarily for Providers that are adding some additional feature
 * on-top of an existing provider such as a discovery based provider that only needs to
 * pass along discovered remote peer information.
 *
 * @param <E> the Type of the Provider instance being wrapped.
 */
public class ProviderWrapper<E extends Provider> implements Provider, ProviderListener {

    protected final E next;
    protected ProviderListener listener;

    public ProviderWrapper(E next) {
        this.next = next;
        this.next.setProviderListener(this);
    }

    @Override
    public void connect(JmsConnectionInfo connectionInfo) throws ProviderException {
        next.connect(connectionInfo);
    }

    @Override
    public void start() throws ProviderException, IllegalStateException {
        if (this.listener == null) {
            throw new IllegalStateException("Cannot start with null ProviderListener");
        }
        next.start();
    }

    @Override
    public void close() {
        next.close();
    }

    @Override
    public URI getRemoteURI() {
        return next.getRemoteURI();
    }

    @Override
    public List<URI> getAlternateURIs() {
        return next.getAlternateURIs();
    }

    @Override
    public void create(JmsResource resource, AsyncResult request) throws ProviderException {
        next.create(resource, request);
    }

    @Override
    public void start(JmsResource resource, AsyncResult request) throws ProviderException {
        next.start(resource, request);
    }

    @Override
    public void stop(JmsResource resource, AsyncResult request) throws ProviderException {
        next.stop(resource, request);
    }

    @Override
    public void destroy(JmsResource resourceId, AsyncResult request) throws ProviderException {
        next.destroy(resourceId, request);
    }

    @Override
    public void send(JmsOutboundMessageDispatch envelope, AsyncResult request) throws ProviderException {
        next.send(envelope, request);
    }

    @Override
    public void acknowledge(JmsSessionId sessionId, ACK_TYPE ackType, AsyncResult request) throws ProviderException {
        next.acknowledge(sessionId, ackType, request);
    }

    @Override
    public void acknowledge(JmsInboundMessageDispatch envelope, ACK_TYPE ackType, AsyncResult request) throws ProviderException {
        next.acknowledge(envelope, ackType, request);
    }

    @Override
    public void commit(JmsTransactionInfo transactionInfo, JmsTransactionInfo nextTransactionInfo, AsyncResult request) throws ProviderException {
        next.commit(transactionInfo, nextTransactionInfo, request);
    }

    @Override
    public void rollback(JmsTransactionInfo transactionInfo, JmsTransactionInfo nextTransactionInfo, AsyncResult request) throws ProviderException {
        next.rollback(transactionInfo, nextTransactionInfo, request);
    }

    @Override
    public void recover(JmsSessionId sessionId, AsyncResult request) throws ProviderException {
        next.recover(sessionId, request);
    }

    @Override
    public void unsubscribe(String subscription, AsyncResult request) throws ProviderException {
        next.unsubscribe(subscription, request);
    }

    @Override
    public void pull(JmsConsumerId consumerId, long timeout, AsyncResult request) throws ProviderException {
        next.pull(consumerId, timeout, request);
    }

    @Override
    public JmsMessageFactory getMessageFactory() {
        return next.getMessageFactory();
    }

    @Override
    public ProviderFuture newProviderFuture() {
        return next.newProviderFuture();
    }

    @Override
    public ProviderFuture newProviderFuture(ProviderSynchronization synchronization) {
        return next.newProviderFuture(synchronization);
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
    public void onInboundMessage(JmsInboundMessageDispatch envelope) {
        listener.onInboundMessage(envelope);
    }

    @Override
    public void onCompletedMessageSend(JmsOutboundMessageDispatch envelope) {
        listener.onCompletedMessageSend(envelope);
    }

    @Override
    public void onFailedMessageSend(JmsOutboundMessageDispatch envelope, ProviderException cause) {
        listener.onFailedMessageSend(envelope, cause);
    }

    @Override
    public void onConnectionInterrupted(URI remoteURI) {
        listener.onConnectionInterrupted(remoteURI);
    }

    @Override
    public void onConnectionRecovery(Provider provider) throws Exception {
        listener.onConnectionRecovery(provider);
    }

    @Override
    public void onConnectionRecovered(Provider provider) throws Exception {
        listener.onConnectionRecovered(provider);
    }

    @Override
    public void onConnectionRestored(URI remoteURI) {
        listener.onConnectionRestored(remoteURI);
    }

    @Override
    public void onConnectionEstablished(URI remoteURI) {
        listener.onConnectionEstablished(this.next.getRemoteURI());
    }

    @Override
    public void onConnectionFailure(ProviderException ex) {
        listener.onConnectionFailure(ex);
    }

    @Override
    public void onResourceClosed(JmsResource resource, ProviderException cause) {
        listener.onResourceClosed(resource, cause);
    }

    @Override
    public void onProviderException(ProviderException cause) {
        listener.onProviderException(cause);
    }

    /**
     * @return the wrapped Provider.
     */
    public Provider getNext() {
        return next;
    }
}
