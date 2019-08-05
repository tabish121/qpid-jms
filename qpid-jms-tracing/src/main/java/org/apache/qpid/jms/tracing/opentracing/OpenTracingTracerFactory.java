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

import java.net.URI;

import org.apache.qpid.jms.tracing.JmsTracer;
import org.apache.qpid.jms.tracing.JmsTracerFactory;

import io.jaegertracing.Configuration;
import io.jaegertracing.Configuration.ReporterConfiguration;
import io.jaegertracing.Configuration.SamplerConfiguration;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

public class OpenTracingTracerFactory extends JmsTracerFactory {

    private static final String JAEGER_SERVICE_NAME = "JAEGER_SERVICE_NAME";

    @Override
    public JmsTracer createTracer(URI remoteURI, String name) throws Exception {
        //TODO: URI option to specify avoiding the GlobalTracer if required?

        OpenTracingPolicy policy = new OpenTracingPolicy();

        // TODO Extract tracing options under jms.tracer and throw if not all consumed

        if (!policy.ignoreGlobalTracer() && GlobalTracer.isRegistered()) {
            return new OpenTracingTracer(policy, GlobalTracer.get(), false);
        } else {
            Tracer tracer = createNewTracer();
            return new OpenTracingTracer(policy, tracer, true);
        }
    }

    private static Tracer createNewTracer() throws Exception {
        //TODO: this is a hack...remove and defer to actual JAEGER env prop config.
        //      ...or, perhaps add URI option to configure it, and thus allow use of Configuration.fromEnv(serviceName) if set?
        String serviceName = System.getProperty(JAEGER_SERVICE_NAME, System.getenv(JAEGER_SERVICE_NAME));
        if (serviceName == null) {
            serviceName = "some-jms-amqp-serviceName";
        }

        Configuration configuration = Configuration.fromEnv(serviceName); //TODO: change to variant that doesnt take serviceName, delete config adjustments below.

        SamplerConfiguration samplerConfig = SamplerConfiguration.fromEnv();//TODO: delete, only present for dev/testing
        samplerConfig.withType("const");//TODO: delete, only present for dev/testing
        samplerConfig.withParam(1);//TODO: delete, only present for dev/testing

        ReporterConfiguration reporterConfig = ReporterConfiguration.fromEnv();//TODO: delete, only present for dev/testing
        reporterConfig.withLogSpans(true);//TODO: delete, only present for dev/testing

        configuration.withReporter(reporterConfig);//TODO: delete, only present for dev/testing
        configuration.withSampler(samplerConfig);//TODO: delete, only present for dev/testing

        return configuration.getTracer();
    }
}
