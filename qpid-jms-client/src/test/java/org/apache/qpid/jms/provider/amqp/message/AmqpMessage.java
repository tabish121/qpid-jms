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
package org.apache.qpid.jms.provider.amqp.message;

import java.util.Arrays;
import java.util.HashMap;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.Encoder;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.decoders.ProtonDecoderFactory;
import org.apache.qpid.protonj2.codec.encoders.ProtonEncoderFactory;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.messaging.ApplicationProperties;
import org.apache.qpid.protonj2.types.messaging.DeliveryAnnotations;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.Header;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.apache.qpid.protonj2.types.messaging.Properties;
import org.apache.qpid.protonj2.types.messaging.Section;

public class AmqpMessage {

    public static final int DEFAULT_PRIORITY = 4;

    private Header header;
    private DeliveryAnnotations deliveryAnnotations;
    private MessageAnnotations messageAnnotations;
    private Properties properties;
    private ApplicationProperties applicationProperties;
    private Footer footer;
    private Section<?> body;

    //----- Message Header API

    public boolean durable() {
        return header == null ? Header.DEFAULT_DURABILITY : header.isDurable();
    }

    public AmqpMessage durable(boolean durable) {
        lazyCreateHeader().setDurable(durable);
        return this;
    }

    public byte priority() {
        return header == null ? Header.DEFAULT_PRIORITY : header.getPriority();
    }

    public AmqpMessage priority(byte priority) {
        lazyCreateHeader().setPriority(priority);
        return this;
    }

    public long timeToLive() {
        return header == null ? Header.DEFAULT_TIME_TO_LIVE : header.getTimeToLive();
    }

    public AmqpMessage timeToLive(long timeToLive) {
        lazyCreateHeader().setTimeToLive(timeToLive);
        return this;
    }

    public boolean firstAcquirer() {
        return header == null ? Header.DEFAULT_FIRST_ACQUIRER : header.isFirstAcquirer();
    }

    public AmqpMessage firstAcquirer(boolean firstAcquirer) {
        lazyCreateHeader().setFirstAcquirer(firstAcquirer);
        return this;
    }

    public long deliveryCount() {
        return header == null ? Header.DEFAULT_DELIVERY_COUNT : header.getDeliveryCount();
    }

    public AmqpMessage deliveryCount(long deliveryCount) {
        lazyCreateHeader().setDeliveryCount(deliveryCount);
        return this;
    }

    //----- Message Properties access

    public Object messageId() {
        return properties != null ? properties.getMessageId() : null;
    }

    public AmqpMessage messageId(Object messageId) {
        lazyCreateProperties().setMessageId(messageId);
        return this;
    }

    public byte[] userId() {
        byte[] copyOfUserId = null;
        if (properties != null && properties.getUserId() != null) {
            copyOfUserId = properties.getUserId().arrayCopy();
        }

        return copyOfUserId;
    }

    public AmqpMessage userId(byte[] userId) {
        lazyCreateProperties().setUserId(new Binary(Arrays.copyOf(userId, userId.length)));
        return this;
    }

    public String to() {
        return properties != null ? properties.getTo() : null;
    }

    public AmqpMessage to(String to) {
        lazyCreateProperties().setTo(to);
        return this;
    }

    public String subject() {
        return properties != null ? properties.getSubject() : null;
    }

    public AmqpMessage subject(String subject) {
        lazyCreateProperties().setSubject(subject);
        return this;
    }

    public String replyTo() {
        return properties != null ? properties.getReplyTo() : null;
    }

    public AmqpMessage replyTo(String replyTo) {
        lazyCreateProperties().setReplyTo(replyTo);
        return this;
    }

    public Object correlationId() {
        return properties != null ? properties.getCorrelationId() : null;
    }

    public AmqpMessage correlationId(Object correlationId) {
        lazyCreateProperties().setCorrelationId(correlationId);
        return this;
    }

    public String contentType() {
        return properties != null ? properties.getContentType() : null;
    }

    public AmqpMessage contentType(String contentType) {
        lazyCreateProperties().setContentType(contentType);
        return this;
    }

    public String contentEncoding() {
        return properties != null ? properties.getContentEncoding() : null;
    }

    public AmqpMessage contentEncoding(String contentEncoding) {
        lazyCreateProperties().setContentEncoding(contentEncoding);
        return this;
    }

    public long absoluteExpiryTime() {
        return properties != null ? properties.getAbsoluteExpiryTime() : null;
    }

    public AmqpMessage absoluteExpiryTime(long expiryTime) {
        lazyCreateProperties().setAbsoluteExpiryTime(expiryTime);
        return this;
    }

    public long creationTime() {
        return properties != null ? properties.getCreationTime() : null;
    }

    public AmqpMessage creationTime(long createTime) {
        lazyCreateProperties().setCreationTime(createTime);
        return this;
    }

    public String groupId() {
        return properties != null ? properties.getGroupId() : null;
    }

    public AmqpMessage groupId(String groupId) {
        lazyCreateProperties().setGroupId(groupId);
        return this;
    }

    public int groupSequence() {
        return properties != null ? (int) properties.getGroupSequence() : null;
    }

    public AmqpMessage groupSequence(int groupSequence) {
        lazyCreateProperties().setGroupSequence(groupSequence);
        return this;
    }

    public String replyToGroupId() {
        return properties != null ? properties.getReplyToGroupId() : null;
    }

    public AmqpMessage replyToGroupId(String replyToGroupId) {
        lazyCreateProperties().setReplyToGroupId(replyToGroupId);
        return this;
    }

    //----- Delivery Annotations Access

    public Object deliveryAnnotations(String key) {
        Object value = null;
        if (deliveryAnnotations != null && deliveryAnnotations.getValue() != null) {
            value = deliveryAnnotations.getValue().get(Symbol.valueOf(key));
        }

        return value;
    }

    public boolean hasDeliveryAnnotations(String key) {
        if (deliveryAnnotations != null && deliveryAnnotations.getValue() != null) {
            return deliveryAnnotations.getValue().containsKey(Symbol.valueOf(key));
        } else {
            return false;
        }
    }

    public AmqpMessage deliveryAnnotation(String key, Object value) {
        lazyCreateDeliveryAnnotations().getValue().put(Symbol.valueOf(key),value);
        return this;
    }

    //----- Message Annotations Access

    public Object messageAnnotation(String key) {
        Object value = null;
        if (messageAnnotations != null) {
            value = messageAnnotations.getValue().get(Symbol.valueOf(key));
        }

        return value;
    }

    public boolean hasMessageAnnotation(String key) {
        if (messageAnnotations != null && messageAnnotations.getValue() != null) {
            return messageAnnotations.getValue().containsKey(Symbol.valueOf(key));
        } else {
            return false;
        }
    }

    public AmqpMessage messageAnnotation(String key, Object value) {
        lazyCreateMessageAnnotations().getValue().put(Symbol.valueOf(key),value);
        return this;
    }

    //----- Application Properties Access

    public Object applicationProperty(String key) {
        Object value = null;
        if (applicationProperties != null) {
            value = applicationProperties.getValue().get(key);
        }

        return value;
    }

    public boolean hasApplicationProperty(String key) {
        if (applicationProperties != null && applicationProperties.getValue() != null) {
            return applicationProperties.getValue().containsKey(key);
        } else {
            return false;
        }
    }

    public AmqpMessage applicationProperty(String key, Object value) {
        lazyCreateApplicationProperties().getValue().put(key,value);
        return this;
    }

    //----- Footer Access

    public Object footer(String key) {
        Object value = null;
        if (footer != null) {
            value = footer.getValue().get(Symbol.valueOf(key));
        }

        return value;
    }

    public boolean hasFooter(String key) {
        if (footer != null && footer.getValue() != null) {
            return footer.getValue().containsKey(Symbol.valueOf(key));
        } else {
            return false;
        }
    }

    public AmqpMessage footer(String key, Object value) {
        lazyCreateFooter().getValue().put(Symbol.valueOf(key),value);
        return this;
    }

    //----- Message body access

    public Section<?> body() {
        return body;
    }

    public AmqpMessage body(Section<?> body) {
        this.body = body;
        return this;
    }

    //----- Access to proton resources

    Header getHeader() {
        return header;
    }

    AmqpMessage setHeader(Header header) {
        this.header = header;
        return this;
    }

    DeliveryAnnotations getDeliveryAnnotations() {
        return deliveryAnnotations;
    }

    AmqpMessage setDeliveryAnnotations(DeliveryAnnotations annotations) {
        this.deliveryAnnotations = annotations;
        return this;
    }

    MessageAnnotations getMessageAnnotations() {
        return messageAnnotations;
    }

    AmqpMessage setMessageAnnotations(MessageAnnotations annotations) {
        this.messageAnnotations = annotations;
        return this;
    }

    Properties getProperties() {
        return properties;
    }

    AmqpMessage setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    ApplicationProperties getApplicationProperties() {
        return applicationProperties;
    }

    AmqpMessage setApplicationProperties(ApplicationProperties applicationProperties) {
        this.applicationProperties = applicationProperties;
        return this;
    }

    Footer getFooter() {
        return footer;
    }

    AmqpMessage setFooter(Footer footer) {
        this.footer = footer;
        return this;
    }

    //----- Encode and Decode static methods

    public static ProtonBuffer encodeMessage(AmqpMessage message) {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Encoder encoder = ProtonEncoderFactory.create();
        EncoderState encoderState = encoder.newEncoderState();

        Header header = message.getHeader();
        DeliveryAnnotations deliveryAnnotations = message.getDeliveryAnnotations();
        MessageAnnotations messageAnnotations = message.getMessageAnnotations();
        Properties properties = message.getProperties();
        ApplicationProperties applicationProperties = message.getApplicationProperties();
        Section<?> body = message.body();
        Footer footer = message.getFooter();

        if (header != null) {
            encoder.writeObject(buffer, encoderState, header);
        }
        if (deliveryAnnotations != null) {
            encoder.writeObject(buffer, encoderState, deliveryAnnotations);
        }
        if (messageAnnotations != null) {
            encoder.writeObject(buffer, encoderState, messageAnnotations);
        }
        if (properties != null) {
            encoder.writeObject(buffer, encoderState, properties);
        }
        if (applicationProperties != null) {
            encoder.writeObject(buffer, encoderState, applicationProperties);
        }
        if (body != null) {
            encoder.writeObject(buffer, encoderState, body);
        }
        if (footer != null) {
            encoder.writeObject(buffer, encoderState, footer);
        }

        return buffer;
    }

    public static AmqpMessage decode(byte[] array, int arrayOffset, int length) {
        return decode(ProtonByteBufferAllocator.DEFAULT.wrap(array, arrayOffset, length));
    }

    public static AmqpMessage decode(ProtonBuffer buffer) {
        Decoder decoder = ProtonDecoderFactory.create();
        DecoderState state = decoder.newDecoderState();

        Header header = null;
        DeliveryAnnotations deliveryAnnotations = null;
        MessageAnnotations messageAnnotations = null;
        Properties properties = null;
        ApplicationProperties applicationProperties = null;
        Section<?> body = null;
        Footer footer = null;
        Section<?> section = null;

        while (buffer.isReadable()) {
            section = decoder.readObject(buffer, state, Section.class);

            switch (section.getType()) {
                case Header:
                    header = (Header) section;
                    break;
                case DeliveryAnnotations:
                    deliveryAnnotations = (DeliveryAnnotations) section;
                    break;
                case MessageAnnotations:
                    messageAnnotations = (MessageAnnotations) section;
                    break;
                case Properties:
                    properties = (Properties) section;
                    break;
                case ApplicationProperties:
                    applicationProperties = (ApplicationProperties) section;
                    break;
                case Data:
                case AmqpSequence:
                case AmqpValue:
                    body = section;
                    break;
                case Footer:
                    footer = (Footer) section;
                    break;
                default:
                    throw new RuntimeException("Unknown Message Section forced decode abort.");
            }
        }

        final AmqpMessage result = new AmqpMessage();

        result.setHeader(header);
        result.setDeliveryAnnotations(deliveryAnnotations);
        result.setMessageAnnotations(messageAnnotations);
        result.setProperties(properties);
        result.setApplicationProperties(applicationProperties);
        result.setFooter(footer);
        result.body(body);

        return result;
    }


    //----- Internal API

    private Header lazyCreateHeader() {
        if (header == null) {
            header = new Header();
        }

        return header;
    }

    private Properties lazyCreateProperties() {
        if (properties == null) {
            properties = new Properties();
        }

        return properties;
    }

    private ApplicationProperties lazyCreateApplicationProperties() {
        if (applicationProperties == null) {
            applicationProperties = new ApplicationProperties(new HashMap<>());
        }

        return applicationProperties;
    }

    private MessageAnnotations lazyCreateMessageAnnotations() {
        if (messageAnnotations == null) {
            messageAnnotations = new MessageAnnotations(new HashMap<>());
        }

        return messageAnnotations;
    }

    private DeliveryAnnotations lazyCreateDeliveryAnnotations() {
        if (deliveryAnnotations == null) {
            deliveryAnnotations = new DeliveryAnnotations(new HashMap<>());
        }

        return deliveryAnnotations;
    }

    private Footer lazyCreateFooter() {
        if (footer == null) {
            footer = new Footer(new HashMap<>());
        }

        return footer;
    }
}
