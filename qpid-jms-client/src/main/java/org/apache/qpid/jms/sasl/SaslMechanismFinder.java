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
package org.apache.qpid.jms.sasl;

import java.util.Collection;
import java.util.Collections;

import org.apache.qpid.jms.util.FactoryFinder;
import org.apache.qpid.jms.util.ResourceNotFoundException;
import org.apache.qpid.protonj2.engine.sasl.client.Mechanism;
import org.apache.qpid.protonj2.engine.sasl.client.SaslCredentialsProvider;
import org.apache.qpid.protonj2.engine.sasl.client.SaslMechanismSelector;
import org.apache.qpid.protonj2.engine.util.StringUtils;
import org.apache.qpid.protonj2.types.Symbol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to find a SASL Mechanism that most closely matches the preferred set
 * of Mechanisms supported by the remote peer.
 *
 * The Matching mechanism is chosen by first find all instances of SASL
 * mechanism types that are supported on the remote peer, and then making a
 * final selection based on the Mechanism in the found set that has the
 * highest priority value.
 */
public class SaslMechanismFinder extends SaslMechanismSelector {

    private static final Logger LOG = LoggerFactory.getLogger(SaslMechanismFinder.class);

    private static final FactoryFinder<MechanismFactory> MECHANISM_FACTORY_FINDER =
        new FactoryFinder<MechanismFactory>(MechanismFactory.class,
            "META-INF/services/" + SaslMechanismFinder.class.getPackage().getName().replace(".", "/") + "/");

    @SuppressWarnings("unchecked")
    public SaslMechanismFinder(Collection<String> allowed) {
        super(allowed != null ? StringUtils.toSymbolSet(allowed) : Collections.EMPTY_SET);
    }

    @Override
    protected Mechanism createMechanism(Symbol name, SaslCredentialsProvider credentials) {
        MechanismFactory factory = findMechanismFactory(name.toString());
        if (factory != null) {
            return factory.createMechanism();
        } else {
            return super.createMechanism(name, credentials);
        }
    }

    /**
     * Searches for a MechanismFactory by using the scheme from the given name.
     *
     * The search first checks the local cache of mechanism factories before moving on
     * to search in the classpath.
     *
     * @param name
     *        The name of the authentication mechanism to search for.
     *
     * @return a mechanism factory instance matching the name, or null if none was created.
     */
    protected static MechanismFactory findMechanismFactory(String name) {
        if (name == null || name.isEmpty()) {
            LOG.warn("No SASL mechanism name was specified");
            return null;
        }

        MechanismFactory factory = null;
        try {
            factory = MECHANISM_FACTORY_FINDER.newInstance(name);
        } catch (ResourceNotFoundException rnfe) {
            LOG.trace("Unknown SASL mechanism: [" + name + "]");
        } catch (Exception e) {
            LOG.warn("Caught exception while finding factory for SASL mechanism {}: {}", name, e.getMessage());
        }

        return factory;
    }
}
