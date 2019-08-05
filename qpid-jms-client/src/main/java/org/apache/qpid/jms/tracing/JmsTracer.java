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
package org.apache.qpid.jms.tracing;

public interface JmsTracer {

    enum DeliveryOutcome {
        DELIVERED,
        EXPIRED,
        REDELIVERIES_EXCEEDED,
        USER_ERROR
    }

    /**
     * Provides a hint to the client if the tracer is actually performing tracing or not
     * which allows short circuit of some tracing related code.
     *
     * @return if the Tracer instance is enabled or performing any actual tracing.
     */
    boolean isTracing();

    JmsTracingPolicy getTracingPolicy();

    void initSend(TracableMessage message, String address);

    void completeSend(TracableMessage message, String outcome);

    void syncReceive(TracableMessage message, DeliveryOutcome outcome);

    void asyncDeliveryInit(TracableMessage message);

    void asyncDeliveryComplete(TracableMessage message, DeliveryOutcome outcome);

    void close();

    void extract(TracableMessage message);

}
