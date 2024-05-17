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
package org.apache.qpid.jms.meta;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.message.JmsMessageIDBuilder;

public final class JmsProducerInfo extends JmsAbstractResource implements Comparable<JmsProducerInfo> {

    private final JmsProducerId producerId;
    private final JmsMessageIDBuilder messageIDBuilder;

    private JmsDestination destination;
    private boolean presettle;
    private boolean compressionEnabled;
    private volatile boolean compressionSupported;

    public JmsProducerInfo(JmsProducerId producerId) {
        this(producerId, JmsMessageIDBuilder.BUILTIN.DEFAULT.createBuilder());
    }

    public JmsProducerInfo(JmsProducerId producerId, JmsMessageIDBuilder messageIDBuilder) {
        if (producerId == null) {
            throw new IllegalArgumentException("Producer ID cannot be null");
        }

        if (messageIDBuilder == null) {
            throw new IllegalArgumentException("Message ID Builder cannot be null");
        }

        this.producerId = producerId;
        this.messageIDBuilder = messageIDBuilder;
    }

    public JmsProducerInfo copy() {
        JmsProducerInfo info = new JmsProducerInfo(producerId, messageIDBuilder);
        copy(info);
        return info;
    }

    public void copy(JmsProducerInfo info) {
        info.destination = destination;
    }

    @Override
    public JmsProducerId getId() {
        return producerId;
    }

    public JmsSessionId getParentId() {
        return producerId.getParentId();
    }

    public JmsDestination getDestination() {
        return destination;
    }

    public void setDestination(JmsDestination destination) {
        this.destination = destination;
    }

    /**
     * @return the presettle mode of this producer.
     */
    public boolean isPresettle() {
        return presettle;
    }

    /**
     * Sets the presettle mode of the producer, when true the producer will be created
     * as a presettled producer and all messages it sends will be settled before dispatch.
     *
     * @param presettle
     *      the presettle option to set on this producer.
     */
    public void setPresettle(boolean presettle) {
        this.presettle = presettle;
    }

    /**
     * @return <code>true</code> if the producer is enabled for sending messages with compressed bodies.
     */
    public boolean isCompressionEnabled() {
        return compressionEnabled;
    }

    /**
     * Sets the compression enabled value for this producer. When <code>true</code> the producer
     * is enabled to send messages with compressed message bodies following the AMQP JMS message
     * compression format.
     *
     * @param enabled
     * 		set to <code>true</code> if a producer can send messages compressed.
     */
    public void setCompressionEnabled(boolean enabled) {
        this.compressionEnabled = enabled;
    }

    public JmsMessageIDBuilder getMessageIDBuilder() {
        return messageIDBuilder;
    }

    /**
     * @return <code>true</code> if the producer link supports sending messages with compressed bodies.
     */
    public boolean isCompressionSupported() {
        return compressionSupported;
    }

    /**
     * Sets if the current link supports compression following an attach. If compression was
     * not enabled then this will always be false, otherwise it will be false if the attach
     * outcome indicates that compression is unavailable on this remote.
     */
    public void setCompressionSupported(boolean supported) {
        this.compressionSupported = supported;
    }

    @Override
    public String toString() {
        return "JmsProducerInfo { " + getId() + ", destination = " + getDestination() + " }";
    }

    @Override
    public int hashCode() {
        return producerId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        JmsProducerInfo other = (JmsProducerInfo) obj;
        return producerId.equals(other.producerId);
    }

    @Override
    public int compareTo(JmsProducerInfo other) {
        return producerId.compareTo(other.producerId);
    }

    @Override
    public void visit(JmsResourceVistor vistor) throws Exception {
        vistor.processProducerInfo(this);
    }
}
