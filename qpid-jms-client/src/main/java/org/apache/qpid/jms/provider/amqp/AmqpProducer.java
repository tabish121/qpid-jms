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
package org.apache.qpid.jms.provider.amqp;

import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsProducerId;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.protonj2.engine.Sender;

/**
 * Base class for Producer instances.
 */
public interface AmqpProducer extends AmqpEndpoint<Sender> {

    /**
     * Sends the given message
     *
     * @param envelope
     *        The envelope that contains the message and it's targeted destination.
     * @param request
     *        The AsyncRequest that will be notified on send success or failure.
     *
     * @throws ProviderException if an error occurs sending the message
     */
    void send(JmsOutboundMessageDispatch envelope, AsyncResult request) throws ProviderException;

    /**
     * @return true if this is an anonymous producer or false if fixed to a given destination.
     */
    boolean isAnonymous();

    /**
     * @return the JmsProducerId that was assigned to this AmqpProducer.
     */
    JmsProducerId getProducerId();

    /**
     * @return true if the producer should presettle all sent messages.
     */
    boolean isPresettle();

}
