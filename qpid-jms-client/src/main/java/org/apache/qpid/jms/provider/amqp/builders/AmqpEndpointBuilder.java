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
package org.apache.qpid.jms.provider.amqp.builders;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.meta.JmsResource.ResourceState;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.ProviderListener;
import org.apache.qpid.jms.provider.amqp.AmqpEndpoint;
import org.apache.qpid.jms.provider.amqp.AmqpProvider;
import org.apache.qpid.jms.provider.amqp.AmqpSupport;
import org.apache.qpid.jms.provider.exceptions.ProviderExceptionSupport;
import org.apache.qpid.jms.provider.exceptions.ProviderOperationTimedOutException;
import org.apache.qpid.jms.provider.exceptions.ProviderResourceClosedException;
import org.apache.qpid.protonj2.engine.Endpoint;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base for all AmqpResource builders.
 *
 * @param <T> The Type of resource that will be created.
 * @param <P> The Type of this resource's parent.
 * @param <I> The Type of JmsResource used to describe the target resource.
 * @param <E> The AMQP Endpoint that the target resource encapsulates.
 */
public abstract class AmqpEndpointBuilder<T extends AmqpEndpoint<?>, P, I extends JmsResource, E extends Endpoint<E>> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpEndpointBuilder.class);

    protected AsyncResult request;
    protected ScheduledFuture<?> requestTimeoutTask;

    protected AmqpProvider provider;
    protected E endpoint;
    protected final P parent;
    protected final I resourceInfo;

    protected Consumer<T> resourceConsumer;

    public AmqpEndpointBuilder(AmqpProvider provider, P parent, I resourceInfo) {
        this.provider = provider;
        this.parent = parent;
        this.resourceInfo = resourceInfo;
    }

    /**
     * Called to initiate the process of building the resource type that is
     * managed by this builder.  The resource is created and the open process
     * occurs asynchronously.  If the resource is successfully opened it will
     * added to its parent resource for use.
     *
     * @param request
     *      The request that initiated the resource creation.
     */
    public void buildEndpoint(final AsyncResult request) {
        buildEndpoint(request, null);
    }

    /**
     * Called to initiate the process of building the resource type that is
     * managed by this builder.  The resource is created and the open process
     * occurs asynchronously.  If the resource is successfully opened it will
     * added to its parent resource for use.
     *
     * @param request
     *      The request that initiated the resource creation.
     * @param resourceConsumer
     * 		The {@link Consumer} that want to know when the resource is ready for use.
     */
    public void buildEndpoint(final AsyncResult request, Consumer<T> resourceConsumer) {
        this.request = request;
        this.resourceConsumer = resourceConsumer;

        // Store the request with the provider for failure if connection drops
        provider.addToFailOnConnectionDropTracking(request);

        // Create the local end of the manage resource.
        endpoint = createEndpoint(resourceInfo);
        endpoint.setLinkedResource(this)
                .localOpenHandler(this::handleLocalOpen)
                .localCloseHandler(this::handleLocalClose)
                .openHandler(this::handleRemoteOpen)
                .closeHandler(this::handleRemotecClose)
                .engineShutdownHandler(this::handleEngineShutdown)
                .open();
    }

    //----- Event handlers ---------------------------------------------------//

    private void handleLocalOpen(E endpoint) {
        // Possible the remote already sent an open for this resource so check for that before
        // scheduling any timeouts on the open.
        if (!endpoint.isRemotelyOpen() && !endpoint.isRemotelyClosed()) {
            if (getOpenTimeout() != JmsConnectionInfo.INFINITE) {
                requestTimeoutTask = provider.getScheduler().schedule(() -> {
                    handleEndpointOpenTimedOut(getProvider(), getEndpoint());
                }, getOpenTimeout(), TimeUnit.MILLISECONDS);
            }
        } else {
            handleEndpointAlreadyOpened(getProvider(), getEndpoint());
        }
    }

    private void handleLocalClose(E endpoint) {
        LOG.trace("Builder closed endpoint {} without successful open", getEndpoint());
    }

    private void handleRemoteOpen(E endpoint) {
        provider.removeFromFailOnConnectionDropTracking(request);

        processEndpointRemotelyOpened(getEndpoint(), getResourceInfo());

        if (!isClosePending()) {
            if (requestTimeoutTask != null) {
                requestTimeoutTask.cancel(false);
                requestTimeoutTask = null;
            }

            // Create the requested resource now which will serve as a hand off to that
            // endpoint implementation of the proton Endpoint instance we created.
            getEndpoint().setLinkedResource(null)
                         .localCloseHandler(null)
                         .localOpenHandler(null)
                         .openHandler(null)
                         .closeHandler(null)
                         .engineShutdownHandler(null);

            resourceInfo.setState(ResourceState.OPEN);
            T resource = createResource(getParent(), getResourceInfo(), getEndpoint());

            if (resourceConsumer != null) {
                resourceConsumer.accept(resource);
                resourceConsumer = null;
            }

            request.onSuccess();
        }
    }

    private void handleRemotecClose(E endpoint) {
        provider.removeFromFailOnConnectionDropTracking(request);

        getResourceInfo().setState(ResourceState.REMOTELY_CLOSED);

        // If the resource being built is closed during the creation process
        // then this is always an error.

        processEndpointRemotelyClosed(getEndpoint(), getResourceInfo());

        final ProviderException openError = getOpenAbortExceptionFromRemote();

        if (requestTimeoutTask != null) {
            requestTimeoutTask.cancel(false);
        }

        LOG.warn("Open of resource:({}) failed: {}", resourceInfo, openError.getMessage());

        // This resource is now terminated.
        getEndpoint().close()
                     .setLinkedResource(null)
                     .localOpenHandler(null)
                     .localCloseHandler(null)
                     .openHandler(null)
                     .closeHandler(null)
                     .engineShutdownHandler(null);
        getRequest().onFailure(openError);
    }

    private void handleEngineShutdown(Engine engine) {
        if (requestTimeoutTask != null) {
            requestTimeoutTask.cancel(false);
            requestTimeoutTask = null;
        }

        try {
            getEndpoint().close()
                         .setLinkedResource(null)
                         .localOpenHandler(null)
                         .localCloseHandler(null)
                         .openHandler(null)
                         .closeHandler(null)
                         .engineShutdownHandler(null);
        } catch (Exception ignore) {
        }

        if (request != null) {
            request.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(provider.getFailureCause()));
            request = null;
        }
    }

    private void handleEndpointOpenTimedOut(AmqpProvider provider, E endpoint) {
        resourceInfo.setState(ResourceState.CLOSED);

        // Perform any post processing relating to closure during creation attempt
        processEndpointRemotelyClosed(getEndpoint(), getResourceInfo());

        final ProviderException error = getDefaultEndpointOpenTimedOutException();

        if (requestTimeoutTask != null) {
            requestTimeoutTask.cancel(false);
        }

        LOG.warn("Open of resource:({}) timed out: {}", resourceInfo, error.getMessage());

        // This resource is now terminated.
        getEndpoint().close()
                     .setLinkedResource(null)
                     .localOpenHandler(null)
                     .localCloseHandler(null)
                     .openHandler(null)
                     .closeHandler(null)
                     .engineShutdownHandler(null);

        getRequest().onFailure(error);
    }

    private void handleEndpointAlreadyOpened(AmqpProvider provider, E endpoint) {
        if (endpoint.isRemotelyOpen()) {
            handleRemoteOpen(endpoint);
        } else {
            handleRemotecClose(endpoint);
        }
    }

    protected final boolean hasRemoteError() {
        return getEndpoint().getRemoteCondition() != null &&
               getEndpoint().getRemoteCondition().getCondition() != null;
    }

    //----- Implementation methods used to customize the build process -------//

    /**
     * Given the resource information provided create and configure the local endpoint
     * whose open phase is managed by this builder.
     *
     * @return a new endpoint to be managed.
     */
    protected abstract E createEndpoint(I resourceInfo);

    /**
     * Create the managed resource instance.
     *
     * @param parent
     *      The parent of the newly created resource.
     * @param resourceInfo
     *      The resource information used to configure the resource.
     * @param endpoint
     *      The local endpoint for the managed resource to wrap.
     *
     * @return the resource instance who open life-cycle is managed by this builder.
     */
    protected abstract T createResource(P parent, I resourceInfo, E endpoint);

    /**
     * If the resource was opened but its current state indicates a close is pending
     * then we do no need to proceed further into the resource creation process.  Each
     * endpoint build must implement this and examine the opened endpoint to determine
     * if a close frame will follow the open.
     *
     * @return true if the resource state indicates it will be immediately closed.
     */
    protected abstract boolean isClosePending();

    /**
     * Called once an endpoint has been opened remotely to give the subclasses a
     * place to perform any follow-on processing or setup steps before the operation
     * is deemed to have been completed and success or failure is signaled based
     * on the {@link #isClosePending()} method and the remote error state.
     *
     * @param endpoint
     * 		The {@link Endpoint} that has been remotely opened.
     * @param resourceInfo
     * 		The {@link JmsResource} configuration for the opened {@link Endpoint}.
     */
    protected void processEndpointRemotelyOpened(E endpoint, I resourceInfo) {
        // Nothing done in default implementation.
    }

    /**
     * Called if endpoint opening process fails in order to give the subclasses a
     * place to perform any follow-on processing or tear down steps before the operation
     * is deemed to have been completed and failure is signaled.
     *
     * @param endpoint
     * 		The {@link Endpoint} that has been remotely opened.
     * @param resourceInfo
     * 		The {@link JmsResource} configuration for the opened {@link Endpoint}.
     */
    protected void processEndpointRemotelyClosed(E endpoint, I resourceInfo) {
        // Nothing done in default implementation.
    }

    /**
     * When aborting the open operation, and there isn't an error condition,
     * provided by the peer, the returned exception will be used instead.
     * A subclass may override this method to provide alternative behavior.
     *
     * @return an Exception to describes the open failure for this resource.
     */
    protected ProviderException getDefaultOpenAbortException() {
        return new ProviderException("Open failed unexpectedly.");
    }

    /**
     * When aborting the open operation, this method will attempt to create an appropriate
     * exception from the remote {@link ErrorCondition} if one is set and will revert to
     * creating the default variant if not.
     *
     * @return an Exception to describes the open failure for this resource.
     */
    protected ProviderException getOpenAbortExceptionFromRemote() {
        if (hasRemoteError()) {
            return AmqpSupport.convertToNonFatalException(getProvider(), getEndpoint().getRemoteCondition());
        } else {
            return getDefaultOpenAbortException();
        }
    }

    /**
     * When the {@link Endpoint} is unexpectedly closed and no {@link ErrorCondition} is not
     * provided this {@link Exception} will be used to signal the {@link ProviderListener}
     * that a resource was remotely closed.
     *
     * @return the default error to use when a resource is remotely closed without cause.
     */
    protected ProviderException getDefaultRemotelyClosedException() {
        return new ProviderResourceClosedException("Resource was remotely closed without cause given.");
    }

    /**
     * When the {@link Endpoint} is unexpectedly closed and no {@link ErrorCondition} is
     * provided this {@link Exception} will be used to signal the {@link ProviderListener}
     * that a resource was remotely closed.
     *
     * @return the default error to use when a resource is remotely closed without cause.
     */
    protected ProviderException getRemotelyClosedExceptionFromRemote() {
        if (hasRemoteError()) {
            return AmqpSupport.convertToNonFatalException(provider, getEndpoint().getRemoteCondition());
        } else {
            return getDefaultRemotelyClosedException();
        }
    }

    /**
     * When the open of an {@link Endpoint} times out waiting for a remote response this method will
     * be called to obtain an appropriate {@link ProviderException} that describes the error.
     *
     * @return the default error to use when a resource open times out.
     */
    protected ProviderException getDefaultEndpointOpenTimedOutException() {
        return new ProviderOperationTimedOutException("Request to open resource " + getResourceInfo() + " timed out");
    }

    /**
     * Returns the configured time before the open of the {@link Endpoint} is considered
     * to have failed.  Subclasses can override this method to provide a value more
     * appropriate to the resource being built.
     *
     * @return the configured timeout before the open of the resource fails.
     */
    protected long getOpenTimeout() {
        return getProvider().getRequestTimeout();
    }

    /**
     * Returns the configured time before the close of the {@link Endpoint} is considered
     * to have failed.  Subclasses can override this method to provide a value more
     * appropriate to the resource being built.
     *
     * @return the configured timeout before the open of the resource fails.
     */
    protected long getCloseTimeout() {
        return getProvider().getRequestTimeout();
    }

    //----- Public access methods for the managed resources ------------------//

    public AmqpProvider getProvider() {
        return provider;
    }

    public E getEndpoint() {
        return endpoint;
    }

    public AsyncResult getRequest() {
        return request;
    }

    public P getParent() {
        return parent;
    }

    public I getResourceInfo() {
        return resourceInfo;
    }
}
