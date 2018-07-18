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
import java.util.concurrent.locks.LockSupport;

import org.apache.qpid.jms.util.IOExceptionSupport;

/**
 * An optimized version of a ProviderFuture that makes use of spin waits and other
 * methods of reacting to asynchronous completion in a more timely manner.
 */
public class ProgressiveProviderFuture extends ProviderFuture {

    // Using a progressive wait strategy helps to avoid wait happening before
    // completion and avoid using expensive thread signaling
    private static final int SPIN_COUNT = 10;
    private static final int YIELD_COUNT = 100;
    private static final int TINY_PARK_COUNT = 1000;
    private static final int TINY_PARK_NANOS = 1;
    private static final int SMALL_PARK_COUNT = 101_000;
    private static final int SMALL_PARK_NANOS = 10_000;

    public ProgressiveProviderFuture() {
        this(null);
    }

    public ProgressiveProviderFuture(ProviderSynchronization synchronization) {
        super(synchronization);
    }

    @Override
    public void quickSync() throws IOException {
        try {
            if (isComplete()) {
                failOnError();
                return;
            }

            int idleCount = 0;

            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException();
            }

            while (true) {
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
}
