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

import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.meta.JmsResource.ResourceState;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.ProviderListener;
import org.apache.qpid.jms.provider.exceptions.ProviderExceptionSupport;
import org.apache.qpid.jms.provider.exceptions.ProviderOperationTimedOutException;
import org.apache.qpid.jms.provider.exceptions.ProviderResourceClosedException;
import org.apache.qpid.protonj2.engine.Endpoint;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base for all {@link AmqpEndpoint} implementations to extend.
 *
 * This abstract class wraps up the basic state management bits so that the concrete
 * object don't have to reproduce it.  Provides hooks for the subclasses to initialize
 * and shutdown.
 *
 * @param <R> The {@link JmsResource} type that describe this resource.
 * @param <E> The AMQP {@link Endpoint} that this resource encapsulates.
 */
public abstract class AmqpAbstractEndpoint<R extends JmsResource, E extends Endpoint<E>> implements AmqpEndpoint<E> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpAbstractEndpoint.class);

    protected ProviderException failureCause;
    protected AsyncResult closeRequest;
    protected ScheduledFuture<?> closeTimeoutTask;

    private final AmqpProvider provider;
    private final E endpoint;
    private final R resourceInfo;

    public AmqpAbstractEndpoint(AmqpProvider provider, R resourceInfo, E endpoint) {
        this.provider = provider;
        this.endpoint = endpoint;
        this.resourceInfo = resourceInfo;
        this.resourceInfo.getId().setProviderHint(this);

        this.endpoint.setLinkedResource(this)
                     .localCloseHandler(this::handleLocalClose)
                     .closeHandler(this::handleRemotecClose)
                     .engineShutdownHandler(this::handleEngineShutdown);
    }

    @Override
    public void close(AsyncResult request) {
        resourceInfo.setState(ResourceState.CLOSED);

        // If the endpoint was already locally closed by us it means the caller won't
        // ever need to know if completion succeed or failed as we already indicated
        // to them a remote close if that was the reason so just complete their request.
        if (getEndpoint().isLocallyClosed()) {
            request.onSuccess();
        } else {
            closeRequest = request;
            doCloseOfWrappedEndpoint();
        }
    }

    @Override
    public void close(ProviderException cause) {
        Objects.requireNonNull(cause, "The causal exception for this force closure cannot be null");

        // If already remotely closed then we would have already handled all close operations
        // and this method should no-op.
        if (!getEndpoint().isRemotelyClosed()) {
            resourceInfo.setState(ResourceState.CLOSED);
            failureCause = cause;

            if (closeTimeoutTask != null) {
                closeTimeoutTask.cancel(false);
                closeTimeoutTask = null;
            }

            // Disable any local close handling and then trigger subclass close processing we will
            // not need to do normal remote close processing either just update state to reflect it
            // happened for completeness.
            getEndpoint().localCloseHandler(null);
            getEndpoint().closeHandler(endpoint -> resourceInfo.setState(ResourceState.REMOTELY_CLOSED));

            doCloseOfWrappedEndpoint();
            processEndpointClosed();

            if (isAwaitingRemoteClose()) {
                LOG.debug("{} is now closed: ", this);
                closeRequest.onFailure(cause);
                closeRequest = null;
            } else {
                signalEndpointRemotelyClosed();
            }
        } else if (!isClosed()) {
            // Already remotely closed so just remove this endpoint from the engine
            // by closing it down other remote closed processing already would have
            // signaled endpoint closure.
            doCloseOfWrappedEndpoint();
        }
    }

    @Override
    public boolean isAwaitingRemoteClose() {
        return closeRequest != null;
    }

    @Override
    public boolean isOpen() {
        return getEndpoint().isLocallyOpen();
    }

    @Override
    public boolean isClosed() {
        return getEndpoint().isLocallyClosed();
    }

    @Override
    public boolean isRemotelyOpen() {
        return getEndpoint().isRemotelyOpen();
    }

    @Override
    public boolean isRemotelyClosed() {
        return getEndpoint().isRemotelyClosed();
    }

    @Override
    public AmqpProvider getProvider() {
        return provider;
    }

    @Override
    public E getEndpoint() {
        return endpoint;
    }

    public R getResourceInfo() {
        return resourceInfo;
    }

    @Override
    public ProviderException getFailureCause() {
        return failureCause;
    }

    protected void setFailureCause(ProviderException cause) {
        this.failureCause = cause;
    }

    //----- Subclasses must override these to properly create a valid endpoint

    protected long getDefaultCloseTimeout() {
        // Use close timeout for all resource closures and fallback to the request
        // timeout if the close timeout was not set
        long closeTimeout = getProvider().getCloseTimeout();
        if (closeTimeout == JmsConnectionInfo.INFINITE) {
            closeTimeout = getProvider().getRequestTimeout();
        }

        return closeTimeout;
    }

    /**
     * Called when the {@link AmqpEndpoint} is being closed either directly or as a response
     * to a remote close.  The {@link AmqpEndpoint} implementation can override this and perform
     * the correct close operation for the specific {@link Endpoint} being implemented.
     */
    protected void doCloseOfWrappedEndpoint() {
        getEndpoint().close();
    }

    /**
     * When the remote closes the resource and it was not expecting it to be then an error
     * is surfaced to the {@link Provider} indicating that.  This method returns the
     * {@link ProviderException} that will be used to indicate this error.
     *
     * @return the default exception that is used to signal resource remotely closed.
     */
    protected ProviderException createEndpointRemotelyClosedException() {
        if (hasRemoteError()) {
            return AmqpSupport.convertToNonFatalException(provider, getEndpoint().getRemoteCondition());
        } else {
            return createDefaultEndpointRemotelyClosedException();
        }
    }

    /**
     * If the endpoint is remotely closed unexpectedly and the remote did not include an {@link ErrorCondition}
     * this method will be called to allow an {@link Endpoint} to create a default error of the correct type.
     *
     * @return a default {@link ProviderResourceClosedException} if remotely closed but no remote condition given.
     */
    protected ProviderException createDefaultEndpointRemotelyClosedException() {
        return new ProviderResourceClosedException("Remote closed this endpoint unexpectedly: Unknown error from remote peer");
    }

    /**
     * Called after the remote close of this Endpoint arrives.  The subclasses can intercept
     * this event to update their own state data or that of child resources.  If needed the subclass
     * can configure a failure cause which would override the default behavior of not signaling a
     * waiting close request that an error occurred.
     */
    protected void processEndpointClosed() {
        // Nothing to do in this handler, subclass can override to handle
    }

    /**
     * Called from the parent {@link Endpoint} when it has been remotely closed or closed locally
     * by force to allow a child resource to cancel any pending work or otherwise signal that it will
     * not complete work that it has outstanding.
     */
    @Override
    public void processParentEndpointClosed(AmqpEndpoint<?> parent) {
        if (failureCause == null) {
            setFailureCause(parent.getFailureCause());
        }

        processEndpointClosed();
    }

    /**
     * Called after the remote close of this Endpoint arrives and the state has been updated
     * and any child resource notified.  This method should signal the provider or its listeners
     * that the resource has been closed so that the upper layer of the client can react to the
     * remote close.
     *
     * By default this method sends an event to the {@link ProviderListener} that is registered
     * with the {@link Provider} that the resource was remotely closed and includes either a mapped
     * exception from the remote condition or if none was provided it creates a generic default error.
     */
    protected void signalEndpointRemotelyClosed() {
        provider.fireResourceClosed(getResourceInfo(), failureCause);
    }

    protected final boolean hasRemoteError() {
        return getEndpoint().getRemoteCondition() != null && getEndpoint().getRemoteCondition().getCondition() != null;
    }

    //----- Internal event handlers that cannot be overridden

    private void handleLocalClose(E endpoint) {
        // Possible that the remote already closed or the provider is failed in which case there
        // is no expected response from the remote so no need to set timeout.
        if (!getEndpoint().isRemotelyClosed() && !provider.isFailed() && !getEndpoint().getEngine().isShutdown()) {
            long closeTimeout = getDefaultCloseTimeout();
            if (closeTimeout != JmsConnectionInfo.INFINITE) {
                closeTimeoutTask = provider.getScheduler().schedule(() -> {
                    ProviderException error = new ProviderOperationTimedOutException(
                        "Request to close resource " + this + " timed out");
                    LOG.warn("Close of resource:({}) timed out: {}", resourceInfo, error.getMessage());
                    close(error);
                }, closeTimeout, TimeUnit.MILLISECONDS);
            }
        }
    }

    private void handleRemotecClose(E endpoint) {
        getResourceInfo().setState(ResourceState.REMOTELY_CLOSED);

        if (closeTimeoutTask != null) {
            closeTimeoutTask.cancel(false);
            closeTimeoutTask = null;
        }

        // Respond to the remote with the close update.
        doCloseOfWrappedEndpoint();

        // Remote closed when not waiting on it is a failure that should be signaled.
        if (!isAwaitingRemoteClose() && failureCause == null) {
            failureCause = createEndpointRemotelyClosedException();
        }

        processEndpointClosed();

        // Unlink this resource from the endpoint as we are now done.
        getEndpoint().localCloseHandler(null)
                     .closeHandler(null)
                     .engineShutdownHandler(null);

        if (isAwaitingRemoteClose()) {
            // Close was requested so we just answer affirmative regardless of a remote
            // error condition.
            if (failureCause == null) {
                closeRequest.onSuccess();
            } else {
                closeRequest.onFailure(failureCause);
            }
            closeRequest = null;
        } else {
            signalEndpointRemotelyClosed();
        }
    }

    private void handleEngineShutdown(Engine engine) {
        if (closeTimeoutTask != null) {
            closeTimeoutTask.cancel(false);
            closeTimeoutTask = null;
        }

        try {
            getEndpoint().close();
        } catch (Exception ignore) {
        }

        if (closeRequest != null) {
            closeRequest.onFailure(ProviderExceptionSupport.createNonFatalOrPassthrough(provider.getFailureCause()));
            closeRequest = null;
        }
    }
}
