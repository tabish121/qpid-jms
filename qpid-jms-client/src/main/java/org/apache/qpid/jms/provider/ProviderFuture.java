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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.LockSupport;

import org.apache.qpid.jms.util.IOExceptionSupport;

/**
 * Asynchronous Provider Future class.
 */
public class ProviderFuture implements AsyncResult {

    // Using a progressive wait strategy helps to avoid await happening before countDown
    // and avoids expensive thread signaling
    private static final int SPIN_COUNT = 10;
    private static final int YIELD_COUNT = 100;
    private static final int TINY_PARK_COUNT = 1000;
    private static final int TINY_PARK_NANOS = 1;
    private static final int SMALL_PARK_COUNT = 101000;
    private static final int SMALL_PARK_NANOS = 10000;

    // States used to track progress of this future
    private static final int INCOMPLETE = 0;
    private static final int COMPLETING = 1;
    private static final int SUCCESS = 2;
    private static final int FAILURE = 3;

    private static final AtomicIntegerFieldUpdater<ProviderFuture> COMPLETER_FIELD_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ProviderFuture.class,"complete");

    private volatile int complete = INCOMPLETE;
    private volatile Throwable error;

    private int waiting;

    private final ProviderSynchronization synchronization;

    public ProviderFuture() {
        this(null);
    }

    public ProviderFuture(ProviderSynchronization synchronization) {
        this.synchronization = synchronization;
    }

    @Override
    public boolean isComplete() {
        return complete > COMPLETING;
    }

    @Override
    public void onFailure(Throwable result) {
        if (COMPLETER_FIELD_UPDATER.compareAndSet(this, INCOMPLETE, COMPLETING)) {
            error = result;
            if (synchronization != null) {
                synchronization.onPendingFailure(error);
            }

            COMPLETER_FIELD_UPDATER.lazySet(this, FAILURE);

            synchronized(this) {
                if (waiting > 0) {
                    notifyAll();
                }
            }
        }
    }

    @Override
    public void onSuccess() {
        if (COMPLETER_FIELD_UPDATER.compareAndSet(this, INCOMPLETE, COMPLETING)) {
            if (synchronization != null) {
                synchronization.onPendingSuccess();
            }

            COMPLETER_FIELD_UPDATER.lazySet(this, SUCCESS);

            synchronized(this) {
                if (waiting > 0) {
                    notifyAll();
                }
            }
        }
    }

    /**
     * Timed wait for a response to a Provider operation.
     *
     * @param amount
     *        The amount of time to wait before abandoning the wait.
     * @param unit
     *        The unit to use for this wait period.
     *
     * @return true if the operation succeeded and false if the waiting time elapsed while
     * 	       waiting for the operation to complete.
     *
     * @throws IOException if an error occurs while waiting for the response.
     */
    public boolean sync(long amount, TimeUnit unit) throws IOException {
        try {
            if (isComplete() || amount == 0) {
                failOnError();
                return true;
            }

            final Thread currentThread = Thread.currentThread();
            final long timeout = unit.toNanos(amount);
            long maxParkNanos = timeout / 8;
            maxParkNanos = maxParkNanos > 0 ? maxParkNanos : timeout;
            final long tinyParkNanos = Math.min(maxParkNanos, TINY_PARK_NANOS);
            final long smallParkNanos = Math.min(maxParkNanos, SMALL_PARK_NANOS);
            final long startTime = System.nanoTime();
            int idleCount = 0;

            while (true) {
                if (currentThread.isInterrupted()) {
                    throw new InterruptedException();
                }

                final long elapsed = System.nanoTime() - startTime;
                final long diff = elapsed - timeout;

                if (diff > 0) {
                    failOnError();
                    return isComplete();
                }

                if (isComplete()) {
                    failOnError();
                    return true;
                }

                if (idleCount < SPIN_COUNT) {
                    idleCount++;
                } else if (idleCount < YIELD_COUNT) {
                    Thread.yield();
                    idleCount++;
                } else if (idleCount < TINY_PARK_COUNT) {
                    LockSupport.parkNanos(tinyParkNanos);
                    idleCount++;
                } else if (idleCount < SMALL_PARK_COUNT) {
                    LockSupport.parkNanos(smallParkNanos);
                    idleCount++;
                } else {
                    synchronized (this) {
                        if (isComplete()) {
                            failOnError();
                            return true;
                        }

                        waiting++;
                        try {
                            wait(-diff / 1000000, (int) (-diff % 1000000));
                        } finally {
                            waiting--;
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw IOExceptionSupport.create(e);
        }
    }

    /**
     * Waits for a response to some Provider requested operation.
     *
     * @throws IOException if an error occurs while waiting for the response.
     */
    public void sync() throws IOException {
        try {
            if (isComplete()) {
                failOnError();
                return;
            }

            final Thread currentThread = Thread.currentThread();
            int idleCount = 0;

            while (true) {
                if (currentThread.isInterrupted()) {
                    throw new InterruptedException();
                }

                if (isComplete()) {
                    failOnError();
                    return;
                }

                if (idleCount < SPIN_COUNT) {
                    idleCount++;
                } else if (idleCount < YIELD_COUNT) {
                    Thread.yield();
                    idleCount++;
                } else if (idleCount < TINY_PARK_COUNT) {
                    LockSupport.parkNanos(TINY_PARK_NANOS);
                    idleCount++;
                } else if (idleCount < SMALL_PARK_COUNT) {
                    LockSupport.parkNanos(SMALL_PARK_NANOS);
                    idleCount++;
                } else {
                    synchronized (this) {
                        if (isComplete()) {
                            failOnError();
                            return;
                        }

                        waiting++;
                        try {
                            wait();
                        } finally {
                            waiting--;
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw IOExceptionSupport.create(e);
        }
    }

    private void failOnError() throws IOException {
        Throwable cause = error;
        if (cause != null) {
            throw IOExceptionSupport.create(cause);
        }
    }
}
