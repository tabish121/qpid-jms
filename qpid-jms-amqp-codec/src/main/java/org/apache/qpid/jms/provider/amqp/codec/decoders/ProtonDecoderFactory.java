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
package org.apache.qpid.jms.provider.amqp.codec.decoders;

import org.apache.qpid.jms.provider.amqp.codec.decoders.messaging.AmqpSequenceTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.messaging.AmqpValueTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.messaging.ApplicationPropertiesTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.messaging.DataTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.messaging.DeliveryAnnotationsTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.messaging.FooterTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.messaging.HeaderTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.messaging.MessageAnnotationsTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.messaging.PropertiesTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Array32TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Binary32TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Binary8TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.BooleanFalseTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.BooleanTrueTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.BooleanTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.ByteTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.CharacterTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Decimal128TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Decimal32TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Decimal64TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.DoubleTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.FloatTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Integer32TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.List0TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.List32TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.List8TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.LongTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Map32TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Map8TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.NullTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.ShortTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Array8TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Integer8TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Long8TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.String32TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.String8TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Symbol32TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.Symbol8TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.TimestampTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.UUIDTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.UnsignedByteTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.UnsignedInteger32TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.UnsignedLong64TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.UnsignedShortTypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.UnsignedInteger8TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.UnsignedLong8TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.UnsignedInteger0TypeDecoder;
import org.apache.qpid.jms.provider.amqp.codec.decoders.primitive.UnsignedLong0TypeDecoder;

/**
 * Factory that create and initializes new BuiltinDecoder instances
 */
public class ProtonDecoderFactory {

    private ProtonDecoderFactory() {
    }

    public static ProtonDecoder create() {
        ProtonDecoder decoder = new ProtonDecoder();

        addPrimitiveDecoders(decoder);
        addMessagingTypeDecoders(decoder);

        return decoder;
    }

    private static void addMessagingTypeDecoders(ProtonDecoder decoder) {
        decoder.registerDescribedTypeDecoder(new AmqpSequenceTypeDecoder());
        decoder.registerDescribedTypeDecoder(new AmqpValueTypeDecoder());
        decoder.registerDescribedTypeDecoder(new ApplicationPropertiesTypeDecoder());
        decoder.registerDescribedTypeDecoder(new DataTypeDecoder());
        decoder.registerDescribedTypeDecoder(new DeliveryAnnotationsTypeDecoder());
        decoder.registerDescribedTypeDecoder(new FooterTypeDecoder());
        decoder.registerDescribedTypeDecoder(new HeaderTypeDecoder());
        decoder.registerDescribedTypeDecoder(new MessageAnnotationsTypeDecoder());
        decoder.registerDescribedTypeDecoder(new PropertiesTypeDecoder());
    }

    private static void addPrimitiveDecoders(ProtonDecoder decoder) {
        decoder.registerPrimitiveTypeDecoder(new BooleanTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new BooleanFalseTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new BooleanTrueTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Binary32TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Binary8TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new ByteTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new CharacterTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Decimal32TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Decimal64TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Decimal128TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new DoubleTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new FloatTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new NullTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new UnsignedByteTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new ShortTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new UnsignedShortTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Integer8TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Integer32TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new UnsignedInteger32TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new UnsignedInteger0TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new UnsignedInteger8TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new LongTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Long8TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new UnsignedLong64TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new UnsignedLong0TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new UnsignedLong8TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new String32TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new String8TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Symbol8TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Symbol32TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new UUIDTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new TimestampTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new List0TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new List8TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new List32TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Map8TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Map32TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Array32TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Array8TypeDecoder());
    }
}
