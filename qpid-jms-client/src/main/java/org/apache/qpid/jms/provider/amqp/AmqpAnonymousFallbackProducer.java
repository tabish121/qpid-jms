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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsProducerId;
import org.apache.qpid.jms.meta.JmsProducerInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.WrappedAsyncResult;
import org.apache.qpid.jms.provider.amqp.builders.AmqpProducerBuilder;
import org.apache.qpid.jms.util.IdGenerator;
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
    private final Map<JmsDestination, ProducerTracker> producerCache = new LinkedHashMap<>();
    private final String producerIdKey = producerIdGenerator.generateId();
    private long producerIdCount;
    private final ScheduledFuture<?> cacheProducerTimeoutTask;

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

        final long sweeperInterval = connection.getAnonymousProducerCacheTimeout();
        if (sweeperInterval > 0) {
            LOG.trace("Cached Producer timeout monitoring enabled: interval = {}ms", sweeperInterval);
            cacheProducerTimeoutTask = connection.schedule(new CachedProducerSweeper(), sweeperInterval);
        } else {
            cacheProducerTimeoutTask = null;
        }
    }

    @Override
    public void send(JmsOutboundMessageDispatch envelope, AsyncResult request) throws ProviderException {
        LOG.trace("Started send chain for anonymous producer: {}", getProducerId());

        ProducerTracker producer = producerCache.get(envelope.getDestination());
        if (producer != null && !producer.isClosed()) {
            producer.send(envelope, request);
        } else if (producerCache.size() < connection.getAnonymousProducerCacheSize()) {
            startSendWithNewProducer(envelope, request);
        } else {
            startSendAfterOldProducerEvicted(envelope, request);
        }
    }

    private void startSendAfterOldProducerEvicted(JmsOutboundMessageDispatch envelope, AsyncResult request) throws ProviderException {

    }

    private void startSendWithNewProducer(JmsOutboundMessageDispatch envelope, AsyncResult request) throws ProviderException {
        // Create a new ProducerInfo for the short lived producer that's created to perform the
        // send to the given AMQP target.
        JmsProducerInfo info = new JmsProducerInfo(getNextProducerId());
        info.setDestination(envelope.getDestination());
        info.setPresettle(this.getResourceInfo().isPresettle());

        // We open a Fixed Producer instance with the target destination.  Once it opens
        // it will trigger the open event which will in turn trigger the send event.
        AmqpProducerBuilder builder = new AmqpProducerBuilder(session, info);
        builder.buildResource(new AnonymousOpenRequest(request, builder, envelope));

        getParent().getProvider().pumpToProtonTransport(request);
    }

    @Override
    public void close(AsyncResult request) {
        if (cacheProducerTimeoutTask != null) {
            cacheProducerTimeoutTask.cancel(false);
        }

        for (ProducerTracker producer : producerCache.values()) {
            producer.close();
        }

        producerCache.clear();

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

    private final class AnonymousOpenRequest extends WrappedAsyncResult {

        private final JmsOutboundMessageDispatch envelope;
        private final AmqpProducerBuilder producerBuilder;

        public AnonymousOpenRequest(AsyncResult sendResult, AmqpProducerBuilder producerBuilder, JmsOutboundMessageDispatch envelope) {
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
            final ProducerTracker producer = new ProducerTracker(producerBuilder.getResource(), connection.getAnonymousProducerCacheTimeout());
            producerCache.put(envelope.getDestination(), producer);

            try {
                producer.send(envelope, getWrappedRequest());
            } catch (ProviderException e) {
                super.onFailure(e);
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

    //----- Cached producer close result handler

    private final class CloseRequest implements AsyncResult {

        private final AmqpProducer producer;

        public CloseRequest(AmqpProducer producer) {
            this.producer = producer;
        }

        @Override
        public void onFailure(ProviderException result) {
            LOG.trace("Close of anonymous producer {} failed: {}", producer, result);
            producerCache.remove(producer.getResourceInfo().getDestination());
            producer.getParent().getProvider().fireProviderException(result);
        }

        @Override
        public void onSuccess() {
            LOG.trace("Close of anonymous producer {} complete", producer);
            producerCache.remove(producer.getResourceInfo().getDestination());
        }

        @Override
        public boolean isComplete() {
            return producer.isClosed();
        }
    }

    //----- AmqpProducer wrapper that adds timeout and cache update mechanisms

    private final class ProducerTracker {

        private final AmqpProducer producer;
        private final long maxInactiveTime;

        private long lastActiveTime = System.nanoTime();

        public ProducerTracker(AmqpProducer producer, int maxInactiveTime) {
            this.producer = producer;
            this.maxInactiveTime = maxInactiveTime;
        }

        public boolean isClosed() {
            return producer.isClosed();
        }

        public ProducerTracker send(JmsOutboundMessageDispatch envelope, AsyncResult request) throws ProviderException {
            lastActiveTime = System.nanoTime();
            producer.send(envelope, request);
            return this;
        }

        public boolean isExpired() {
            return (System.nanoTime() - lastActiveTime) > TimeUnit.MILLISECONDS.toNanos(maxInactiveTime);
        }

        public void close() {
            producer.close(new CloseRequest(producer));
            producer.getParent().getProvider().pumpToProtonTransport();
        }
    }

    //----- Timeout task responsible for closing inactive cached producers

    private final class CachedProducerSweeper implements Runnable {

        @Override
        public void run() {
            final List<ProducerTracker> pending = new ArrayList<>(producerCache.values());
            for (ProducerTracker producer : pending) {
                if (producer.isExpired()) {
                    LOG.trace("Cached Producer {} has timed out, initiating close", producer);
                    producer.close();
                }
            }
        }
    }
}
