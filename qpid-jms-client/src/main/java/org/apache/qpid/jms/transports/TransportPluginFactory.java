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
package org.apache.qpid.jms.transports;

import java.io.IOException;

import org.apache.qpid.jms.util.FactoryFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides the basic factory mechanisms for finding TransportPlugin factories.
 */
public abstract class TransportPluginFactory {

    private static final Logger LOG = LoggerFactory.getLogger(TransportPluginFactory.class);

    private static final FactoryFinder<TransportPluginFactory> TRANSPORT_PLUGIN_FACTORY_FINDER =
            new FactoryFinder<TransportPluginFactory>(TransportPluginFactory.class,
                "META-INF/services/" + TransportPluginFactory.class.getPackage().getName().replace(".", "/") + "/plugins/");

    /**
     * Creates the plugin that this Factory was defined for.
     *
     * @return a new instance of the given plugin.
     *
     * @throws Exception if an error occurs creating the plugin.
     */
    public abstract TransportPlugin createPlugin() throws Exception;

    /**
     * Static create method that performs the TransportPluginFactory search and creates the plugin instance.
     *
     * @param pluginName
     *        The transport plugin name used to locate a TransportPluginFactory.
     *
     * @return a new TransportPlugin instance that is ready for configuration and use.
     *
     * @throws Exception if an error occurs while creating the TransportPlugin instance.
     */
    public static TransportPlugin create(String pluginName) throws Exception {
        TransportPlugin result = null;

        try {
            TransportPluginFactory factory = findTransportPluginFactory(pluginName);
            result = factory.createPlugin();
        } catch (Exception ex) {
            LOG.error("Failed to create TransportPlugin instance for {}, due to: {}", pluginName, ex);
            LOG.trace("Error: ", ex);
            throw ex;
        }

        return result;
    }

    /**
     * Searches for a TransportPluginFactory by using the given plugin name.
     *
     * The search first checks the local cache of TransportPlugin factories before moving on
     * to search in the class-path.
     *
     * @param pluginName
     *        The transport plugin name used to locate a TransportPluginFactory.
     *
     * @return a TransportPlugin factory instance matching the plugin name.
     *
     * @throws IOException if an error occurs while locating the factory.
     */
    public static TransportPluginFactory findTransportPluginFactory(String pluginName) throws IOException {
        if (pluginName == null || pluginName.isEmpty()) {
            throw new IOException("No TransportPlugin key specified");
        }

        TransportPluginFactory factory = null;
        try {
            factory = TRANSPORT_PLUGIN_FACTORY_FINDER.newInstance(pluginName);
        } catch (Throwable e) {
            throw new IOException("TransportPlugin type NOT recognized: [" + pluginName + "]", e);
        }

        return factory;
    }
}
