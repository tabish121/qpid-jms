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
package org.apache.qpid.jms.tracing.opentracing;

import org.apache.qpid.jms.tracing.JmsTracingPolicy;

/**
 * Policy class that holds configuration and can apply tracing policy to actions.
 */
public class OpenTracingPolicy implements JmsTracingPolicy {

    private boolean enabled = true;
    private boolean ignoreGlobalTracer;

    @Override
    public boolean isTracingEnabled() {
        return enabled;
    }

    public void setTracingEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean ignoreGlobalTracer() {
        return ignoreGlobalTracer;
    }

    public void setUseGlobalTracer(boolean ignoreGlobalTracer) {
        this.ignoreGlobalTracer = ignoreGlobalTracer;
    }
}
