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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

public class ProviderFutureTest {

    @Test
    public void testIsComplete() {
        ProviderFuture future = new ProviderFuture();

        assertFalse(future.isComplete());
        future.onSuccess();
        assertTrue(future.isComplete());
    }

    @Test(timeout = 10000)
    public void testOnSuccess() {
        ProviderFuture future = new ProviderFuture();

        future.onSuccess();
        try {
            future.sync();
        } catch (IOException cause) {
            fail("Should throw an error");
        }
    }

    @Test(timeout = 90000)
    public void testTimedSync() {
        ProviderFuture future = new ProviderFuture();

        try {
            assertFalse(future.sync(1, TimeUnit.SECONDS));
        } catch (IOException cause) {
            fail("Should throw an error");
        }
    }

    @Test(timeout = 10000)
    public void testOnFailure() {
        ProviderFuture future = new ProviderFuture();
        IOException ex = new IOException();

        future.onFailure(ex);
        try {
            future.sync(5, TimeUnit.SECONDS);
            fail("Should throw an error");
        } catch (IOException cause) {
            assertSame(cause, ex);
        }
    }

    @Test(timeout = 10000)
    public void testOnSuccessCallsSynchronization() {
        final AtomicBoolean syncCalled = new AtomicBoolean(false);
        ProviderFuture future = new ProviderFuture(new ProviderSynchronization() {

            @Override
            public void onPendingSuccess() {
                syncCalled.set(true);
            }

            @Override
            public void onPendingFailure(Throwable cause) {
            }
        });

        future.onSuccess();
        try {
            future.sync(5, TimeUnit.SECONDS);
        } catch (IOException cause) {
            fail("Should throw an error");
        }

        assertTrue("Synchronization not called", syncCalled.get());
    }

    @Test(timeout = 10000)
    public void testOnFailureCallsSynchronization() {
        final AtomicBoolean syncCalled = new AtomicBoolean(false);
        ProviderFuture future = new ProviderFuture(new ProviderSynchronization() {

            @Override
            public void onPendingSuccess() {
            }

            @Override
            public void onPendingFailure(Throwable cause) {
                syncCalled.set(true);
            }
        });

        IOException ex = new IOException();

        future.onFailure(ex);
        try {
            future.sync(5, TimeUnit.SECONDS);
            fail("Should throw an error");
        } catch (IOException cause) {
            assertSame(cause, ex);
        }

        assertTrue("Synchronization not called", syncCalled.get());
    }

    @Test(timeout = 10000)
    public void testSuccessfulStateIsFixed() {
        ProviderFuture future = new ProviderFuture();
        IOException ex = new IOException();

        future.onSuccess();
        future.onFailure(ex);
        try {
            future.sync(5, TimeUnit.SECONDS);
        } catch (IOException cause) {
            fail("Should throw an error");
        }
    }

    @Test(timeout = 10000)
    public void testFailedStateIsFixed() {
        ProviderFuture future = new ProviderFuture();
        IOException ex = new IOException();

        future.onFailure(ex);
        future.onSuccess();
        try {
            future.sync(5, TimeUnit.SECONDS);
            fail("Should throw an error");
        } catch (IOException cause) {
            assertSame(cause, ex);
        }
    }

    @Test(timeout = 10000)
    public void testSyncHandlesInterruption() throws InterruptedException {
        final ProviderFuture future = new ProviderFuture();

        final CountDownLatch syncing = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(1);
        final AtomicBoolean interrupted = new AtomicBoolean(false);

        Thread runner = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    syncing.countDown();
                    future.sync();
                } catch (IOException cause) {
                    if (cause.getCause() instanceof InterruptedException) {
                        interrupted.set(true);
                    }
                } finally {
                    done.countDown();
                }
            }
        });
        runner.start();
        assertTrue(syncing.await(5, TimeUnit.SECONDS));
        runner.interrupt();

        assertTrue(done.await(5, TimeUnit.SECONDS));

        assertTrue(interrupted.get());
    }

    @Test(timeout = 10000)
    public void testTimedSyncHandlesInterruption() throws InterruptedException {
        final ProviderFuture future = new ProviderFuture();

        final CountDownLatch syncing = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(1);
        final AtomicBoolean interrupted = new AtomicBoolean(false);

        Thread runner = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    syncing.countDown();
                    future.sync(20, TimeUnit.SECONDS);
                } catch (IOException cause) {
                    if (cause.getCause() instanceof InterruptedException) {
                        interrupted.set(true);
                    }
                } finally {
                    done.countDown();
                }
            }
        });
        runner.start();
        assertTrue(syncing.await(5, TimeUnit.SECONDS));
        runner.interrupt();

        assertTrue(done.await(5, TimeUnit.SECONDS));

        assertTrue(interrupted.get());
    }
}
