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
package org.apache.qpid.jms.util;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple ThreadFactory object
 */
public class QpidJMSForkJoinThreadFactory implements ForkJoinWorkerThreadFactory {

    private static final Logger LOG = LoggerFactory.getLogger(QpidJMSForkJoinThreadFactory.class);

    private final String threadName;
    private final boolean daemon;
    private static final ForkJoinUncaughtExceptionHandler exHandler = new ForkJoinUncaughtExceptionHandler();

    /**
     * Creates a new Thread factory that will create threads with the
     * given name and daemon state.
     *
     * @param threadName
     * 		the name that will be used for each thread created.
     * @param daemon
     * 		should the created thread be a daemon thread.
     */
    public QpidJMSForkJoinThreadFactory(String threadName, boolean daemon) {
        this.threadName = threadName;
        this.daemon = daemon;
    }

    @Override
    public ForkJoinWorkerThread newThread(ForkJoinPool pool) {

        ForkJoinWorkerThread thread = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
        thread.setName(threadName);
        thread.setDaemon(daemon);
        thread.setUncaughtExceptionHandler(exHandler);

        return thread;
    }

    /**
     * Gets the static {@link UncaughtExceptionHandler} that should be given to the owning
     * {@link ForkJoinPool}.
     *
     * @return a static {@link UncaughtExceptionHandler} that logs errors for later debug.
     */
    public static UncaughtExceptionHandler getUncaughtExceptionHandler() {
        return exHandler;
    }

    private static final class ForkJoinUncaughtExceptionHandler implements UncaughtExceptionHandler {

        @Override
        public void uncaughtException(Thread target, Throwable error) {
            LOG.warn("Thread: {} failed due to an uncaught exception: {}", target.getName(), error.getMessage());
            LOG.trace("Uncaught Stacktrace: ", error);
        }
    }
}