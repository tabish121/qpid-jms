/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.qpid.jms.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton.amqp.Symbol;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QpidJmsTestCase {

    public static final boolean IS_WINDOWS = System.getProperty("os.name", "unknown").toLowerCase().contains("windows");

    private final Logger _logger = LoggerFactory.getLogger(getClass());

    private final Map<String, String> _propertiesSetForTest = new HashMap<String, String>();

    public static final Symbol SOLE_CONNECTION_CAPABILITY = Symbol.valueOf("sole-connection-for-container");
    public static final Symbol ANONYMOUS_RELAY = Symbol.valueOf("ANONYMOUS-RELAY");
    public static final Symbol DELAYED_DELIVERY = Symbol.valueOf("DELAYED_DELIVERY");
    public static final Symbol SHARED_SUBS = Symbol.valueOf("SHARED-SUBS");
    public static final Symbol ANONYMOUS = Symbol.valueOf("ANONYMOUS");
    public static final Symbol PLAIN = Symbol.valueOf("PLAIN");
    public static final Symbol CRAM_MD5 = Symbol.valueOf("CRAM-MD5");
    public static final Symbol SCRAM_SHA_1 = Symbol.valueOf("SCRAM-SHA-1");
    public static final Symbol SCRAM_SHA_256 = Symbol.valueOf("SCRAM-SHA-256");
    public static final Symbol EXTERNAL = Symbol.valueOf("EXTERNAL");
    public static final Symbol XOAUTH2 = Symbol.valueOf("XOAUTH2");

    public static final Symbol CONNECTION_OPEN_FAILED = Symbol.valueOf("amqp:connection-establishment-failed");
    public static final Symbol INVALID_FIELD_KEY = Symbol.valueOf("invalid-field");
    public static final Symbol INVALID_FIELD = Symbol.valueOf("amqp:invalid-field");
    public static final Symbol CONTAINER_ID = Symbol.valueOf("container-id");
    public static final Symbol PATH = Symbol.valueOf("path");
    public static final Symbol SCHEME = Symbol.valueOf("scheme");
    public static final Symbol PORT = Symbol.valueOf("port");
    public static final Symbol NETWORK_HOST = Symbol.valueOf("network-host");
    public static final Symbol OPEN_HOSTNAME = Symbol.valueOf("hostname");
    public static final Symbol DYNAMIC_NODE_LIFETIME_POLICY = Symbol.valueOf("lifetime-policy");
    public static final Symbol PRODUCT = Symbol.valueOf("product");
    public static final Symbol VERSION = Symbol.valueOf("version");
    public static final Symbol PLATFORM = Symbol.valueOf("platform");

    public static final Symbol JMS_DEST_TYPE_MSG_ANNOTATION_SYMBOL = Symbol.valueOf("x-opt-jms-dest");
    public static final Symbol JMS_REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL = Symbol.valueOf("x-opt-jms-reply-to");
    public static final Symbol QUEUE_CAPABILITY = Symbol.valueOf("queue");
    public static final Symbol TOPIC_CAPABILITY = Symbol.valueOf("topic");
    public static final Symbol TEMP_QUEUE_CAPABILITY = Symbol.valueOf("temporary-queue");
    public static final Symbol TEMP_TOPIC_CAPABILITY = Symbol.valueOf("temporary-topic");
    public static final Symbol QUEUE_PREFIX = Symbol.valueOf("queue-prefix");
    public static final Symbol TOPIC_PREFIX = Symbol.valueOf("topic-prefix");

    public static final Symbol SERIALIZED_JAVA_OBJECT_CONTENT_TYPE = Symbol.valueOf("application/x-java-serialized-object");
    public static final Symbol OCTET_STREAM_CONTENT_TYPE = Symbol.valueOf("application/octet-stream");

    public static final Symbol JMS_MSG_TYPE = Symbol.valueOf("x-opt-jms-msg-type");
    public static final Symbol JMS_DELIVERY_TIME = Symbol.valueOf("x-opt-delivery-time");

    @Rule
    public TestName _testName = new TestName();

    /**
     * Set a System property for duration of this test only. The tearDown will guarantee to reset the property to its
     * previous value after the test completes.
     *
     * @param property
     *            The property to set
     * @param value
     *            the value to set it to, if null, the property will be cleared
     */
    protected void setTestSystemProperty(final String property, final String value) {
        if (!_propertiesSetForTest.containsKey(property)) {
            // Record the current value so we can revert it later.
            _propertiesSetForTest.put(property, System.getProperty(property));
        }

        if (value == null) {
            System.clearProperty(property);
            _logger.info("Set system property '" + property + "' to be cleared");
        } else {
            System.setProperty(property, value);
            _logger.info("Set system property '" + property + "' to: '" + value + "'");
        }

    }

    /**
     * Restore the System property values that were set by this test run.
     */
    protected void revertTestSystemProperties() {
        if (!_propertiesSetForTest.isEmpty()) {
            for (String key : _propertiesSetForTest.keySet()) {
                String value = _propertiesSetForTest.get(key);
                if (value != null) {
                    System.setProperty(key, value);
                    _logger.info("Reverted system property '" + key + "' to: '" + value + "'");
                } else {
                    System.clearProperty(key);
                    _logger.info("Reverted system property '" + key + "' to be cleared");
                }
            }

            _propertiesSetForTest.clear();
        }
    }

    @After
    public void tearDown() throws java.lang.Exception {
        _logger.info("========== tearDown " + getTestName() + " ==========");
        revertTestSystemProperties();
    }

    @Before
    public void setUp() throws Exception {
        _logger.info("========== start " + getTestName() + " ==========");
    }

    protected String getTestName() {
        return getClass().getSimpleName() + "." + _testName.getMethodName();
    }
}
