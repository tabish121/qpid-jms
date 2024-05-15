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

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.AMQP_SPEC_MESSAGE_FORMAT;

import java.io.IOException;

import org.apache.qpid.jms.message.facade.JmsMessageFacade;
import org.apache.qpid.jms.provider.amqp.AmqpConsumer;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;

import io.netty.buffer.ByteBuf;

public class AmqpJmsMessageCodec extends AmqpMessageCodec {

    public static final AmqpMessageCodec INSTANCE = new AmqpJmsMessageCodec();

    @Override
    public int getMessageFormat(JmsMessageFacade message) {
        return AMQP_SPEC_MESSAGE_FORMAT; // Non-compressed messages all use format Zero.
    }

    @Override
    public ByteBuf encodeMessage(AmqpJmsMessageFacade message) throws IOException {
        final EncoderDecoderContext context = TLS_CODEC.get();
        final AmqpWritableBuffer buffer = new AmqpWritableBuffer();
        final EncoderImpl encoder = context.encoder;

        encoder.setByteBuffer(buffer);

        final Header header = message.getHeader();
        final DeliveryAnnotations deliveryAnnotations = message.getDeliveryAnnotations();
        final MessageAnnotations messageAnnotations = message.getMessageAnnotations();
        final Properties properties = message.getProperties();
        final ApplicationProperties applicationProperties = message.getApplicationProperties();
        final Section body = message.getBody();
        final Footer footer = message.getFooter();

        if (header != null) {
            encoder.writeObject(header);
        }
        if (deliveryAnnotations != null) {
            encoder.writeObject(deliveryAnnotations);
        }
        if (messageAnnotations != null) {
            // Ensure annotations contain required message type and destination type data
            AmqpDestinationHelper.setReplyToAnnotationFromDestination(message.getReplyTo(), messageAnnotations);
            AmqpDestinationHelper.setToAnnotationFromDestination(message.getDestination(), messageAnnotations);
            messageAnnotations.getValue().put(AmqpMessageSupport.JMS_MSG_TYPE, message.getJmsMsgType());
            encoder.writeObject(messageAnnotations);
        } else {
            buffer.put(getCachedMessageAnnotationsBuffer(message, context));
        }
        if (properties != null) {
            encoder.writeObject(properties);
        }
        if (applicationProperties != null) {
            encoder.writeObject(applicationProperties);
        }
        if (body != null) {
            encoder.writeObject(body);
        }
        if (footer != null) {
            encoder.writeObject(footer);
        }

        encoder.setByteBuffer((WritableBuffer) null);

        return buffer.getBuffer();
    }

    @Override
    public AmqpJmsMessageFacade decodeMessage(AmqpConsumer consumer, ReadableBuffer messageBytes, int messageFormat) throws IOException {
        final DecoderImpl decoder = getDecoder();

        decoder.setBuffer(messageBytes);

        Header header = null;
        DeliveryAnnotations deliveryAnnotations = null;
        MessageAnnotations messageAnnotations = null;
        Properties properties = null;
        ApplicationProperties applicationProperties = null;
        Section body = null;
        Footer footer = null;
        Section section = null;

        while (messageBytes.hasRemaining()) {
            section = (Section) decoder.readObject();

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
                    throw new IOException("Unknown Message Section forced decode abort.");
            }
        }

        decoder.setByteBuffer(null);

        // First we try the easy way, if the annotation is there we don't have to work hard.
        AmqpJmsMessageFacade result = createFromMsgAnnotation(messageAnnotations);
        if (result == null) {
            // Next, match specific section structures and content types
            result = createWithoutAnnotation(body, properties);
        }

        if (result != null) {
            result.setHeader(header);
            result.setDeliveryAnnotations(deliveryAnnotations);
            result.setMessageAnnotations(messageAnnotations);
            result.setProperties(properties);
            result.setApplicationProperties(applicationProperties);
            result.setBody(body);
            result.setFooter(footer);
            result.initialize(consumer);

            return result;
        }

        throw new IOException("Could not create a JMS message from incoming message");
    }
}
