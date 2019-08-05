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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.qpid.jms.tracing.JmsTracer;
import org.apache.qpid.jms.tracing.JmsTracingPolicy;
import org.apache.qpid.jms.tracing.TracableMessage;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMap;
import io.opentracing.tag.Tags;

public class OpenTracingTracer implements JmsTracer {

    private static final String SEND_SPAN_CONTEXT_KEY = "send";
    private static final String RECEIVE_SPAN_CONTEXT_KEY = "receive";
    private static final String ASYNC_DELIVERY_SPAN = "onMessageSpan";
    private static final String ASYNC_DELIVERY_SCOPE = "onMessageScope";

    private Tracer tracer;
    private boolean closeUnderlyingTracer;
    private final OpenTracingPolicy policy;

    public OpenTracingTracer(OpenTracingPolicy policy, Tracer tracer, boolean closeUnderlyingTracer) {
        this.policy = policy;
        this.tracer = tracer;
        this.closeUnderlyingTracer = closeUnderlyingTracer;
    }

    @Override
    public boolean isTracing() {
        return policy.isTracingEnabled();
    }

    @Override
    public JmsTracingPolicy getTracingPolicy() {
        return policy;
    }

    @Override
    public void initSend(TracableMessage message, String address) {
        Span span = tracer.buildSpan("amqp-delivery-send").start();
        span.setTag(Tags.SPAN_KIND, Tags.SPAN_KIND_PRODUCER);
        span.setTag(Tags.MESSAGE_BUS_DESTINATION, address);
        span.setTag("inserted.automatically", "message-tracing"); // TODO: needed?

        tracer.inject(span.context(), Format.Builtin.TEXT_MAP, new TextMap() {

            @Override
            public void put(String key, String value) {
                message.setTracingAnnotation(key, value);
            }

            @Override
            public Iterator<Entry<String, String>> iterator() {
                throw new UnsupportedOperationException();
            }
        });

        message.setTracingContext(SEND_SPAN_CONTEXT_KEY, span);
    }

    @Override
    public void completeSend(TracableMessage message, String outcome) {
        if (message != null) {
            Object cachedSpan = message.getTracingContext("send");
            if (cachedSpan != null) {
                Span span = (Span) cachedSpan;

                Map<String, String> fields = new HashMap<>();
                fields.put("event", "delivery settled");
                fields.put("state", outcome == null ? "null" : outcome);

                span.log(fields);

                span.finish();
            }
        }
    }

    @Override
    public void extract(TracableMessage message) {
        // TODO: try implementing one of the TextMapAdapters instead of this hack to see if this could be made simpler ?
        Map<String, String> annotations = new HashMap<>();

        message.filterTracingAnnotations((key, value) -> {
            if (value instanceof String) {
                annotations.put(key, (String) value);
            }
        });

        message.setTracingContext(RECEIVE_SPAN_CONTEXT_KEY, tracer.extract(Format.Builtin.TEXT_MAP, new TextMap() {

            @Override
            public Iterator<Entry<String, String>> iterator() {
                return annotations.entrySet().iterator();
            }

            @Override
            public void put(String key, String value) {
                message.setTracingAnnotation(key, value);
            }
        }));
    }

    @Override
    public void syncReceive(TracableMessage message, DeliveryOutcome outcome) {
        Object spanContext = message.getTracingContext(RECEIVE_SPAN_CONTEXT_KEY);

        // TODO: should we still build the span if the context is null, in case there is
        // an active span tracing from the application side?
        if (spanContext != null) {
            SpanContext context = (SpanContext) spanContext;
            SpanBuilder spanBuilder = tracer.buildSpan(RECEIVE_SPAN_CONTEXT_KEY)
                                            .asChildOf(context)
                                            .withTag(Tags.SPAN_KIND, Tags.SPAN_KIND_CONSUMER);

            Span span = spanBuilder.start();
            span.finish();
        }
    }

    @Override
    public void asyncDeliveryInit(TracableMessage message) {
        Object spanContext = message.getTracingContext(RECEIVE_SPAN_CONTEXT_KEY);
        // TODO: should we still build the span if the context is null, to always set an
        // active span if there is a tracer?
        if (spanContext != null) {
            SpanContext context = (SpanContext) spanContext;
            Tracer.SpanBuilder spanBuilder = tracer.buildSpan("onMessage")
                                                   .asChildOf(context)
                                                   .withTag(Tags.SPAN_KIND, Tags.SPAN_KIND_CONSUMER);

            Span span = spanBuilder.start();
            Scope scope = tracer.activateSpan(span);

            message.setTracingAnnotation(ASYNC_DELIVERY_SPAN, span);
            message.setTracingAnnotation(ASYNC_DELIVERY_SCOPE, scope);
        } else {
            throw new RuntimeException("TODO");// TODO
        }
    }

    @Override
    public void asyncDeliveryComplete(TracableMessage message, DeliveryOutcome outcome) {
        Object spanContext = message.getTracingContext(RECEIVE_SPAN_CONTEXT_KEY);
        // TODO: should we still build the span if the context is null, to always set an
        // active span if there is a tracer?
        if (spanContext != null) {
            Span span = (Span) message.removeTracingAnnotation(ASYNC_DELIVERY_SPAN);
            Scope scope = (Scope) message.removeTracingAnnotation(ASYNC_DELIVERY_SCOPE);

            if (span != null && scope != null) {
                scope.close();
                span.finish();
            } else {
                // TODO - Invalid state
            }
        } else {
            throw new RuntimeException("TODO");// TODO
        }
    }

    @Override
    public synchronized void close() {
        if (closeUnderlyingTracer) {
            tracer.close();
        }
    }
}
