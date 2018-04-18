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

/**
 * Provides an interface for plugins that can be used to augment the
 * Transports with additional configuration or feature support.
 */
public interface TransportPlugin {

    /**
     * Returns true if this plugin is compatible with the given {@link Transport}
     * instance.  When a plugin is found to not be applicable an error is signalled
     * to the user indicating they have improperly configured their Transport.
     *
     * @param target
     * 		The Transport that this plugin is beeing applied to.
     *
     * @return true if the plugin supports the given Transport.
     */
    boolean isApplicable(Transport target);

    /**
     * Returns the prefix used for configuration elements of this plugin.
     * <p>
     * The Transports are configured using URI options.  This method returns the prefix
     * used to namespace the options for this plugin.  The root prefix for transport
     * options is 'transport.*' and the plugin options will be treated as an extension
     * of that namespace, e.g 'transport.<plugin-prefix>.configurationOption'.
     *
     * @return the prefix used to identify options applicable to this plugin.
     */
    String getConfigurationPrefix();

    /**
     * Called before a connection attempt is made to allow a plugin to customize the
     * {@link TransportOptions} of the given {@link Transport} based on its own configuration.
     *
     * @param transport
     * 		The {@link Transport} that will be performing a connect attempt
     * @param options
     * 		The {@link TransportOptions} that will be used by the given {@link Transport}
     */
    void configureTransportOptions(Transport transport, TransportOptions options);

}
