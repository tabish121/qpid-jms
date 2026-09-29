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
package org.apache.qpid.jms.provider.discovery;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;

public class DiscoveryIntegrationTest extends QpidJmsTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryIntegrationTest.class);

    @Test
    @Timeout(20)
    public void testCreateConnectionFromFileLocation(@TempDir Path tempDir) throws Exception {
        LOG.info("Discovery path location: {}", tempDir);

        final Path tempFile = tempDir.resolve(getTestName() + ".txt");

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            Files.writeString(tempFile, "amqp://localhost:" + testPeer.getServerPort());

            final String discoveryURI = "discovery:(file://" + tempFile.toAbsolutePath() + ")";

            // Expect connection to the first peer (and have it drop)
            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();

            final ConnectionFactory factory = new JmsConnectionFactory(discoveryURI);
            final Connection connection = factory.createConnection();

            try {
                connection.start();
            } catch (Exception ex) {
                fail("Should have connected to test peer");
            }

            testPeer.waitForAllHandlersToComplete(1000);
            testPeer.expectClose();

            connection.close();

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }

    @Test
    @Timeout(20)
    public void testCreateConnectionFromFileLocationReadsUpdate(@TempDir Path tempDir) throws Exception {
        LOG.info("Discovery path location: {}", tempDir);

        final Path tempFile = tempDir.resolve(getTestName() + ".txt");

        try (TestAmqpPeer testPeer = new TestAmqpPeer();) {
            final String discoveryURI = "discovery:(file://" + tempFile.toAbsolutePath() + "?updateInterval=200)";

            // Expect connection to the first peer (and have it drop)
            testPeer.expectSaslAnonymous();
            testPeer.expectOpen();
            testPeer.expectBegin();

            final ConnectionFactory factory = new JmsConnectionFactory(discoveryURI);
            final Connection connection = factory.createConnection();
            final CountDownLatch starting = new CountDownLatch(1);
            final AtomicReference<Exception> error = new AtomicReference<>();

            ForkJoinPool.commonPool().execute(() -> {
                try {
                    starting.countDown();
                    connection.start();
                } catch (Exception ex) {
                    LOG.error("Unexpected error during connection start", ex);
                    error.set(ex);
                }
            });

            assertTrue(starting.await(2, TimeUnit.SECONDS));

            // Give a short delay to let the transport get going then update the locations file
            Thread.sleep(1);
            Files.writeString(tempFile, "amqp://localhost:" + testPeer.getServerPort());

            testPeer.waitForAllHandlersToComplete(2000);
            testPeer.expectClose();

            connection.close();

            assertNull(error.get());

            testPeer.waitForAllHandlersToComplete(1000);
        }
    }
}
