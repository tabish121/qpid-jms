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
package org.apache.qpid.jms.transports.netty;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.ThreadFactory;

import org.apache.qpid.jms.transports.TransportOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.uring.IoUring;
import io.netty.channel.uring.IoUringIoHandler;
import io.netty.channel.uring.IoUringSocketChannel;

public class IoUringSupport {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static boolean isAvailable(TransportOptions transportOptions) {
        try {
            return transportOptions.isUseIoUring() && IoUring.isAvailable();
        } catch (UnsatisfiedLinkError | Exception e) {
            LOG.debug("Unable to check for io_uring support due to missing class definition", e);
            return false;
        }
    }

    public static EventLoopGroup createGroup(int nThreads, ThreadFactory ioThreadFactory) {
        return new MultiThreadIoEventLoopGroup(nThreads, ioThreadFactory, IoUringIoHandler.newFactory());
    }

    public static void createChannel(Bootstrap bootstrap) {
        bootstrap.channel(IoUringSocketChannel.class);
    }
}
