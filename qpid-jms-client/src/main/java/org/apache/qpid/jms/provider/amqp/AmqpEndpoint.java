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

import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.protonj2.engine.Endpoint;

/**
 * {@link AmqpEndpoint} specification which defines the minimal API that an AMQP
 * {@link Endpoint} abstraction needs to provide to operate in the {@link AmqpProvider}
 * framework/
 *
 * All AMQP types should implement this interface to allow for control of state
 * and configuration details.
 *
 * @param <E> The {@link Endpoint} instance that this abstraction wraps.
 */
public interface AmqpEndpoint<E extends Endpoint<E>> {

    /**
     * Close the resource and signal the {@link AsyncResult} when the remote and
     * local state are both closed.
     *
     * @param request
     * 		The {@link AsyncResult} that is awaiting the close confirmation.
     */
    void close(AsyncResult request);

    /**
     * Immediate close of this {@link AmqpEndpoint} which completes and pending close request
     * and signals the provider immediately if no pending close is present.
     *
     * This method is used in exceptional cases to force close the local end of the {@link Endpoint}
     * regardless of the current state.
     *
     * @param cause
     * 		The error that triggered this immediate close request (cannot be null).
     */
    void close(ProviderException cause);

    /**
     * @return <code>true</code> if this {@link AmqpEndpoint} is awaiting completion of a close request
     */
    boolean isAwaitingRemoteClose();

    /**
     * @return <code>true</code> if the local end of the {@link AmqpEndpoint} is open.
     */
    boolean isOpen();

    /**
     * @return <code>true</code> if the local end of the {@link AmqpEndpoint} is closed.
     */
    boolean isClosed();

    /**
     * @return <code>true</code> if the remote end of the {@link AmqpEndpoint} is open.
     */
    boolean isRemotelyOpen();

    /**
     * @return <code>true</code> if the remote end of the {@link AmqpEndpoint} is closed.
     */
    boolean isRemotelyClosed();

    /**
     * @return the {@link AmqpProvider} that created this resource or its parent.
     */
    AmqpProvider getProvider();

    /**
     * @return the {@link Endpoint} that this object wraps.
     */
    E getEndpoint();

    /**
     * If the {@link AmqpEndpoint} has been closed due to some failure this method will return that
     * {@link ProviderException} which describes the failure cause.
     *
     * @return the {@link ProviderException} that cause this {@link AmqpEndpoint} to become failed.
     */
    ProviderException getFailureCause();

    /**
     * Called when the parent of this resource was closed either explicitly by the client
     * or by a remote close or close timeout operation that is forcing shutdown.
     *
     * @param parent
     * 		The {@link AmqpEndpoint} parent of this one which can carry error information
     */
    void processParentEndpointClosed(AmqpEndpoint<?> parent);

}
