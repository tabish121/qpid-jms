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

import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsProducerId;
import org.apache.qpid.jms.meta.JmsProducerInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.WrappedAsyncResult;
import org.apache.qpid.jms.provider.amqp.builders.AmqpProducerBuilder;
import org.apache.qpid.jms.util.IdGenerator;
import org.apache.qpid.jms.util.LRUCache;
import org.apache.qpid.proton.engine.EndpointState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the case of anonymous JMS MessageProducers.
 *
 * In order to simulate the anonymous producer we must create a sender for each message
 * send attempt and close it following a successful send.
 */
public class AmqpAnonymousFallbackProducer extends AmqpProducer {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpAnonymousFallbackProducer.class);
    private static final IdGenerator producerIdGenerator = new IdGenerator();

    private final AmqpConnection connection;
    private final AnonymousProducerCache producerCache;
    private final String producerIdKey = producerIdGenerator.generateId();
    private long producerIdCount;

    /**
     * Creates the Anonymous Producer object.
     *
     * @param session
     *        the session that owns this producer
     * @param info
     *        the JmsProducerInfo for this producer.
     */
    public AmqpAnonymousFallbackProducer(AmqpSession session, JmsProducerInfo info) {
        super(session, info);

        this.connection = session.getConnection();
        this.producerCache = new AnonymousProducerCache(connection.getAnonymousProducerCacheSize());
    }

    @Override
    public void send(JmsOutboundMessageDispatch envelope, AsyncResult request) throws ProviderException {
        LOG.trace("Started send chain for anonymous producer: {}", getProducerId());

        ProducerTracker producer = producerCache.get(envelope.getDestination());
        if (producer == null) {
            // Create a new ProducerInfo for the short lived producer that's created to perform the
            // send to the given AMQP target.
            JmsProducerInfo info = new JmsProducerInfo(getNextProducerId());
            info.setDestination(envelope.getDestination());
            info.setPresettle(this.getResourceInfo().isPresettle());

            // We open a Fixed Producer instance with the target destination.  Once it opens
            // it will trigger the open event which will in turn trigger the send event.
            AmqpProducerBuilder builder = new AmqpProducerBuilder(session, info);
            builder.buildResource(new AnonymousSendRequest(request, builder, envelope));

            getParent().getProvider().pumpToProtonTransport(request);
        } else {
            producer.cancelPendingClose().send(envelope, request);
        }
    }

    @Override
    public void close(AsyncResult request) {
        for (ProducerTracker producer : producerCache.values()) {
            producer.close();
        }

        request.onSuccess();
    }

    @Override
    public boolean isAnonymous() {
        return true;
    }

    @Override
    public EndpointState getLocalState() {
        return EndpointState.ACTIVE;
    }

    @Override
    public EndpointState getRemoteState() {
        return EndpointState.ACTIVE;
    }

    private JmsProducerId getNextProducerId() {
        return new JmsProducerId(producerIdKey, -1, producerIdCount++);
    }

    //----- AsyncResult objects used to complete the sends -------------------//

    private final class AnonymousSendRequest extends WrappedAsyncResult {

        private final JmsOutboundMessageDispatch envelope;
        private final AmqpProducerBuilder producerBuilder;

        public AnonymousSendRequest(AsyncResult sendResult, AmqpProducerBuilder producerBuilder, JmsOutboundMessageDispatch envelope) {
            super(sendResult);

            this.envelope = envelope;
            this.producerBuilder = producerBuilder;
        }

        @Override
        public void onSuccess() {
            LOG.trace("Open phase of anonymous send complete: {} ", getProducerId());

            // The fixed producer opened so we start tracking it and once send returns indicating that
            // it handled the request (not necessarily sent it but knows it exists) then we start the
            // close clock and if not reused again this producer will eventually time itself out of
            // existence.
            final ProducerTracker producer = new ProducerTracker(producerCache, producerBuilder.getResource());
            producerCache.put(envelope.getDestination(), producer);

            try {
                producer.send(envelope, getWrappedRequest());
            } catch (ProviderException e) {
                super.onFailure(e);
            } finally {
                LOG.trace("Staging close of anonymous fallback producer after send: {} ", getProducerId());
                producer.scheduleClose(connection);
            }
        }

        @Override
        public void onFailure(ProviderException result) {
            LOG.debug("Send failed producer create step in chain: {}", getProducerId());

            // Producer open failed so it was never in the cache, just close it now to ensure
            // that everything is cleaned up.
            CloseRequest close = new CloseRequest(producerBuilder.getResource());
            producerBuilder.getResource().close(close);
            super.onFailure(result);
        }
    }

    private static final class CloseRequest implements AsyncResult {

        private final AmqpProducer producer;

        public CloseRequest(AmqpProducer producer) {
            this.producer = producer;
        }

        @Override
        public void onFailure(ProviderException result) {
            producer.getParent().getProvider().fireProviderException(result);
        }

        @Override
        public void onSuccess() {
            LOG.trace("Close of anonymous producer {} complete", producer);
        }

        @Override
        public boolean isComplete() {
            return producer.isClosed();
        }
    }

    private static final class ProducerTracker {

        private final AnonymousProducerCache cache;
        private final AmqpProducer producer;
        private ScheduledFuture<AmqpFixedProducer> closeFuture;

        public ProducerTracker(AnonymousProducerCache cache, AmqpProducer producer) {
            this.cache = cache;
            this.producer = producer;
        }

        public ProducerTracker scheduleClose(AmqpConnection connection) {
            connection.schedule(() -> close(), connection.getAnonymousProducerCacheTimeout());
            return this;
        }

        public ProducerTracker send(JmsOutboundMessageDispatch envelope, AsyncResult request) throws ProviderException {
            producer.send(envelope, request);
            return this;
        }

        public ProducerTracker cancelPendingClose() {
            if (closeFuture != null) {
                closeFuture.cancel(false);
                closeFuture = null;
            }

            return this;
        }

        public ProducerTracker removeFromCache() {
            cache.remove(producer.getResourceInfo().getDestination());
            return this;
        }

        public void close() {
            removeFromCache();
            cancelPendingClose();
            producer.close(new CloseRequest(producer));
            producer.getParent().getProvider().pumpToProtonTransport();
        }
    }

    private final class AnonymousProducerCache extends LRUCache<JmsDestination, ProducerTracker> {

        private static final long serialVersionUID = 1L;

        public AnonymousProducerCache(int cacheSize) {
            super(cacheSize);
        }

        @Override
        protected void onCacheEviction(Map.Entry<JmsDestination, ProducerTracker> cached) {
            LOG.trace("Producer: {} evicted from producer cache", cached.getValue());

            // Evicted from the cache means to much churn so don't wait any longer, just
            // close the cached producer now and another will take its place when needed.
            cached.getValue().close();
        }
    }
}
