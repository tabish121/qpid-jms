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
package org.apache.qpid.jms.policy;

import java.util.Objects;

import org.apache.qpid.jms.JmsDestination;

import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.Topic;

/**
 * Policy object that allows for configuration of options that affect when
 * a JMS MessageProducer will result in AMQP compressed message sends.
 */
public class JmsDefaultCompressionPolicy implements JmsCompressionPolicy {

    private boolean compressAll;

    private boolean compressAnonymousProducers;
    private boolean compressTopicProducers;
    private boolean compressQueueProducers;

    public JmsDefaultCompressionPolicy() {
    }

    public JmsDefaultCompressionPolicy(JmsDefaultCompressionPolicy policy) {
        this.compressAll = policy.compressAll;
    }

    @Override
    public JmsCompressionPolicy copy() {
        return new JmsDefaultCompressionPolicy(this);
    }

    @Override
    public boolean isCompressionTarget(Session session, JmsDestination destination) {
        if (compressAll) {
            return true;
        } else if (destination == null) {
            return compressAnonymousProducers;
        } else if (destination.isQueue()) {
            return compressQueueProducers;
        } else {
            return compressTopicProducers;
        }
    }

    /**
     * @return <code>true</code> if the policy enables compression globally or not.
     */
    public boolean isCompressAll() {
        return compressAll;
    }

    /**
     * Configures if message compression is enabled globally or not. When enabled any
     * message sent is compressed if the link supports it.
     *
     * @param compressAll
     *      the compressAll value to apply to this policy.
     */
    public void setCompressAll(boolean compressAll) {
        this.compressAll = compressAll;
    }

    /**
     * @return <code>true</code> if the policy enables compression on a {@link MessageProducer} for a {@link Topic}.
     */
    public boolean isCompressTopicProducers() {
        return compressAll || compressTopicProducers;
    }

    /**
     * The compress {@link Topic} producers value to apply. When <code>true</code> any {@link MessageProducer}
     * created to a {@link Topic} destination will have its body compressed if the links supports it.
     *
     * @param compressTopicProducers
     *      the compressTopicProducers value to apply to this policy.
     */
    public void setCompressTopicProducers(boolean compressTopicProducers) {
        this.compressTopicProducers = compressTopicProducers;
    }

    /**
     * @return <code>true</code> if the policy enables compression on a {@link MessageProducer} for a {@link Queue}.
     */
    public boolean isCompressQueueProducers() {
        return compressAll || compressQueueProducers;
    }

    /**
     * The compress {@link Queue} producers value to apply. When <code>true</code> any {@link MessageProducer}
     * created to a {@link Queue} destination will have its body compressed if the links supports it.
     *
     * @param compressQueueProducers
     *      the compressQueueProducers value to apply to this policy.
     */
    public void setCompressQueueProducers(boolean compressQueueProducers) {
        this.compressQueueProducers = compressQueueProducers;
    }

    /**
     * @return <code>true</code> if the policy enables compression on a anonymous {@link MessageProducer}.
     */
    public boolean isCompressAnonymousProducers() {
        return compressAll || compressAnonymousProducers;
    }

    /**
     * The compress anonymous producers value to apply. When <code>true</code> any anonymous (no Destination)
     * {@link MessageProducer} created will have its body compressed if the links supports it.
     *
     * @param compressAnonymousProducers
     *      the compressAnonymousProducers value to apply to this policy.
     */
    public void setCompressAnonymousProducers(boolean compressAnonymousProducers) {
        this.compressAnonymousProducers = compressAnonymousProducers;
    }

    @Override
    public int hashCode() {
        return Objects.hash(compressAll, compressAnonymousProducers, compressQueueProducers, compressTopicProducers);
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

        final JmsDefaultCompressionPolicy other = (JmsDefaultCompressionPolicy) obj;

        return compressAll == other.compressAll &&
               compressAnonymousProducers == other.compressAnonymousProducers &&
               compressQueueProducers == other.compressQueueProducers &&
               compressTopicProducers == other.compressTopicProducers;
    }
}
