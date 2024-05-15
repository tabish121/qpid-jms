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

import org.apache.qpid.proton.amqp.Symbol;

import jakarta.jms.BytesMessage;
import jakarta.jms.MapMessage;
import jakarta.jms.ObjectMessage;
import jakarta.jms.StreamMessage;
import jakarta.jms.TextMessage;

/**
 * Constants used when sending and receiving compressed AMQP JMS messages
 */
public abstract class AmqpJmsCompressedMessageConstants {

    /**
     * AMQP JMS Message compression awareness capability.
     *
     * A capability added to a Sender link desired capabilities on attach which indicates
     * that the sender wishes to send compressed AMQP JMS mapped messages to the remote.
     * If the remote responds by adding the capability to the set of offered capabilities
     * in the Attach response then the Sender can be assured that the remote knows how to
     * handle the compressed messages.
     *
     * An capability added to a Receiver link offered capabilities on attach which indicates
     * that the Receiver is aware of AMQP JMS message compression which allows the Sender to
     * know that it can send already compressed AMQP JMS message or compress existing messages
     * on dispatch and the receiver will be able to handle them, the Sender must add this value
     * to it's desired capabilities in the attach response or the receiver must assume that
     * compressed AMQP JMS message will never be sent across the link.
     */
    public static final Symbol AMQP_JMS_COMPRESSION_AWARE = Symbol.getSymbol("AMQP_JMS_COMPRESSION_AWARE");

    /*
     * Prefix value used on all custom message formats that is the ASF IANA number.
     *
     * https://www.iana.org/assignments/enterprise-numbers/enterprise-numbers
     */
    private static final int QPID_JMS_MESSAGE_FORMAT_PREFIX = 0x468C0000;

    /*
     * Used to indicate that the format contains a compressed BytesMessage
     */
    private static final int COMPRESSED_BYTES_MESSAGE_TYPE = 0x00001100;

    /*
     * Used to indicate that the format contains a compressed MapMessage
     */
    private static final int COMPRESSED_MAP_MESSAGE_TYPE = 0x00001200;

    /*
     * Used to indicate that the format contains a compressed ObjectMessage
     */
    private static final int COMPRESSED_OBJECT_MESSAGE_TYPE = 0x00001300;

    /*
     * Used to indicate that the format contains a compressed StreamMessage
     */
    private static final int COMPRESSED_STREAM_MESSAGE_TYPE = 0x00001400;

    /*
     * Used to indicate that the format contains a compressed TextMessage
     */
    private static final int COMPRESSED_TEXT_MESSAGE_TYPE = 0x00001500;

    /*
     * Indicate version one of the message format
     */
    private static final int COMPRESSED_MESSAGE_FORMAT_V1 = 0x00;

    /**
     * Compressed {@link BytesMessage} format value used when sending compressed messages
     */
    public static final int COMPRESSED_BYTES_MESSAGE_FORMAT = QPID_JMS_MESSAGE_FORMAT_PREFIX |
                                                              COMPRESSED_BYTES_MESSAGE_TYPE |
                                                              COMPRESSED_MESSAGE_FORMAT_V1;

    /**
     * Compressed {@link MapMessage} format value used when sending compressed messages
     */
    public static final int COMPRESSED_MAP_MESSAGE_FORMAT = QPID_JMS_MESSAGE_FORMAT_PREFIX |
                                                            COMPRESSED_MAP_MESSAGE_TYPE |
                                                            COMPRESSED_MESSAGE_FORMAT_V1;

    /**
     * Compressed {@link ObjectMessage} format value used when sending compressed messages
     */
    public static final int COMPRESSED_OBJECT_MESSAGE_FORMAT = QPID_JMS_MESSAGE_FORMAT_PREFIX |
                                                               COMPRESSED_OBJECT_MESSAGE_TYPE |
                                                               COMPRESSED_MESSAGE_FORMAT_V1;

    /**
     * Compressed {@link StreamMessage} format value used when sending compressed messages
     */
    public static final int COMPRESSED_STREAM_MESSAGE_FORMAT = QPID_JMS_MESSAGE_FORMAT_PREFIX |
                                                               COMPRESSED_STREAM_MESSAGE_TYPE |
                                                               COMPRESSED_MESSAGE_FORMAT_V1;

    /**
     * Compressed {@link TextMessage} format value used when sending compressed messages
     */
    public static final int COMPRESSED_TEXT_MESSAGE_FORMAT = QPID_JMS_MESSAGE_FORMAT_PREFIX |
                                                             COMPRESSED_TEXT_MESSAGE_TYPE |
                                                             COMPRESSED_MESSAGE_FORMAT_V1;

}
