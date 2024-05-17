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
package org.apache.qpid.jms.test.testpeer;

import static org.apache.qpid.jms.provider.amqp.AmqpSupport.ANONYMOUS_RELAY;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.DELAYED_DELIVERY;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.DYNAMIC_NODE_LIFETIME_POLICY;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.GLOBAL;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.SHARED;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.SHARED_SUBS;
import static org.apache.qpid.jms.provider.amqp.AmqpSupport.SOLE_CONNECTION_CAPABILITY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLContext;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslServer;

import org.apache.qpid.jms.provider.amqp.AmqpSupport;
import org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper;
import org.apache.qpid.jms.test.testpeer.basictypes.AmqpError;
import org.apache.qpid.jms.test.testpeer.basictypes.ReceiverSettleMode;
import org.apache.qpid.jms.test.testpeer.basictypes.Role;
import org.apache.qpid.jms.test.testpeer.basictypes.SenderSettleMode;
import org.apache.qpid.jms.test.testpeer.basictypes.StdDistMode;
import org.apache.qpid.jms.test.testpeer.basictypes.TerminusDurability;
import org.apache.qpid.jms.test.testpeer.basictypes.TerminusExpiryPolicy;
import org.apache.qpid.jms.test.testpeer.basictypes.TransactionError;
import org.apache.qpid.jms.test.testpeer.describedtypes.Accepted;
import org.apache.qpid.jms.test.testpeer.describedtypes.AttachFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.BeginFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.CloseFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.Coordinator;
import org.apache.qpid.jms.test.testpeer.describedtypes.Declare;
import org.apache.qpid.jms.test.testpeer.describedtypes.Declared;
import org.apache.qpid.jms.test.testpeer.describedtypes.DetachFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.Discharge;
import org.apache.qpid.jms.test.testpeer.describedtypes.DispositionFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.EndFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.FlowFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.FrameDescriptorMapping;
import org.apache.qpid.jms.test.testpeer.describedtypes.OpenFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.Rejected;
import org.apache.qpid.jms.test.testpeer.describedtypes.Released;
import org.apache.qpid.jms.test.testpeer.describedtypes.SaslChallengeFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.SaslMechanismsFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.SaslOutcomeFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.Source;
import org.apache.qpid.jms.test.testpeer.describedtypes.Target;
import org.apache.qpid.jms.test.testpeer.describedtypes.TransferFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.ApplicationPropertiesDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.HeaderDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.MessageAnnotationsDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.PropertiesDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.AttachMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.BeginMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.CloseMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.CoordinatorMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.DeleteOnCloseMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.DetachMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.DispositionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.EndMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.FlowMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.OpenMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.SaslInitMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.SaslResponseMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.SourceMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.TargetMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.TransferMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.sections.TransferPayloadCompositeMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.types.EncodedAmqpValueMatcher;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedShort;
import org.apache.qpid.proton.codec.Data;
import org.apache.qpid.proton.engine.impl.AmqpHeader;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAmqpPeer implements AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TestAmqpPeer.class.getName());

    public static final String MESSAGE_NUMBER = "MessageNumber";

    private static final Symbol ANONYMOUS = Symbol.valueOf("ANONYMOUS");
    private static final Symbol EXTERNAL = Symbol.valueOf("EXTERNAL");
    private static final Symbol PLAIN = Symbol.valueOf("PLAIN");
    private static final Symbol GSSAPI = Symbol.valueOf("GSSAPI");
    private static final Symbol XOAUTH2 = Symbol.valueOf("XOAUTH2");
    private static final UnsignedByte SASL_OK = UnsignedByte.valueOf((byte) 0);
    private static final UnsignedByte SASL_FAIL_AUTH = UnsignedByte.valueOf((byte) 1);
    private static final UnsignedByte SASL_SYS_TEMP = UnsignedByte.valueOf((byte) 4);
    private static final int CONNECTION_CHANNEL = 0;
    private static final int DEFAULT_PRODUCER_CREDIT = 100;
    private static final Symbol[] DEFAULT_DESIRED_CAPABILITIES = new Symbol[] { SOLE_CONNECTION_CAPABILITY, DELAYED_DELIVERY, ANONYMOUS_RELAY, SHARED_SUBS};

    private volatile AssertionError _firstAssertionError = null;
    private final TestAmqpPeerRunner _driverRunnable;
    private final Thread _driverThread;

    /**
     * Guarded by {@link #_handlersLock}
     */
    private final List<Handler> _handlers = new ArrayList<Handler>();
    private final Object _handlersLock = new Object();

    /**
     * Guarded by {@link #_handlersLock}
     */
    private CountDownLatch _handlersCompletedLatch;

    private byte[] _deferredBytes;
    private int _lastInitiatedChannel = -1;
    private UnsignedInteger _lastInitiatedLinkHandle = null;
    private UnsignedInteger _lastInitiatedCoordinatorLinkHandle = null;
    private int advertisedIdleTimeout = 0;
    private AtomicInteger _emptyFrameCount = new AtomicInteger();

    public TestAmqpPeer() throws IOException
    {
        this(null, false);
    }

    public TestAmqpPeer(SSLContext context, boolean needClientCert) throws IOException
    {
        this(context, needClientCert, false);
    }

    public TestAmqpPeer(SSLContext context, boolean needClientCert, boolean sendSaslHeaderPreEmptively) throws IOException
    {
        _driverRunnable = new TestAmqpPeerRunner(this, context, needClientCert);
        _driverRunnable.setSendSaslHeaderPreEmptively(sendSaslHeaderPreEmptively);
        _driverThread = new Thread(_driverRunnable, "MockAmqpPeer-" + _driverRunnable.getServerPort());
        _driverThread.start();
    }

    /**
     * Shuts down the test peer, throwing any Throwable
     * that occurred on the peer, or validating that no
     * unused matchers remain.
     */
    @Override
    public void close() throws Exception
    {
        _driverRunnable.stop();

        try
        {
            _driverThread.join(30000);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
        finally
        {
            AssertionError ae = _firstAssertionError;
            if(ae != null)
            {
                String message = "Assertion failure during test run";
                if(ae.getMessage() != null)
                {
                    message += ": " + ae.getMessage();
                }

                throw new AssertionError(message, _firstAssertionError);
            }

            Throwable throwable = getThrowable();
            if(throwable == null)
            {
                synchronized(_handlersLock)
                {
                    assertThat(_handlers, Matchers.empty());
                }
            }
            else
            {
                //AutoClosable can't handle throwing Throwables, so we wrap it.
                throw new RuntimeException("TestPeer caught throwable during run", throwable);
            }
        }
    }

    public Throwable getThrowable()
    {
        return _driverRunnable.getException();
    }

    public void setSuppressReadExceptionOnClose(boolean suppress)
    {
        _driverRunnable.setSuppressReadExceptionOnClose(suppress);
    }

    public int getServerPort()
    {
        return _driverRunnable.getServerPort();
    }

    public Socket getClientSocket()
    {
        return _driverRunnable.getClientSocket();
    }

    public boolean isSSL()
    {
        return _driverRunnable.isSSL();
    }

    public int getAdvertisedIdleTimeout()
    {
        return advertisedIdleTimeout;
    }

    public void setAdvertisedIdleTimeout(int advertisedIdleTimeout)
    {
        this.advertisedIdleTimeout = advertisedIdleTimeout;
    }

    public int getEmptyFrameCount() {
        return _emptyFrameCount.get();
    }

    public void purgeExpectations() {
        synchronized (_handlersLock) {
            _handlers.clear();
        }
    }

    void receiveHeader(byte[] header)
    {
        Handler handler = getFirstHandler();
        if(handler instanceof HeaderHandler)
        {
            ((HeaderHandler)handler).header(header,this);
            removeFirstHandler();
        }
        else
        {
            throw new IllegalStateException("Received header but the next handler is a " + handler);
        }
    }

    void receiveFrame(int type, int channel, int frameSize, DescribedType describedType, Binary payload)
    {
        Handler handler = getFirstHandler();

        while (handler instanceof FrameHandler && ((FrameHandler) handler).isOptional())
        {
            FrameHandler frameHandler = (FrameHandler) handler;
            if(frameHandler.descriptorMatches(describedType.getDescriptor())){
                LOGGER.info("Optional frame handler matches the descriptor, proceeding to verify it");
                break;
            } else {
                LOGGER.info("Skipping non-matching optional frame handler, received frame descriptor (" + describedType.getDescriptor() + ") does not match handler: " +  frameHandler);
                removeFirstHandler();
                handler = getFirstHandler();
            }
        }

        if(handler == null)
        {
            Object actualDescriptor = describedType.getDescriptor();
            Object mappedDescriptor = FrameDescriptorMapping.lookupMapping(actualDescriptor);

            throw new IllegalStateException("No handler! Received frame, descriptor=" + actualDescriptor + "/" + mappedDescriptor);
        }

        if(handler instanceof FrameHandler)
        {
            ((FrameHandler)handler).frame(type, channel, frameSize, describedType, payload, this);
            removeFirstHandler();
        }
        else
        {
            throw new IllegalStateException("Received frame but the next handler is a " + handler);
        }
    }

    void receiveEmptyFrame(int type, int channel)
    {
        _emptyFrameCount.incrementAndGet();
        LOGGER.debug("Received empty frame");
    }

    private void removeFirstHandler()
    {
        synchronized(_handlersLock)
        {
            Handler h = _handlers.remove(0);
            if(_handlersCompletedLatch != null)
            {
                _handlersCompletedLatch.countDown();
            }
            LOGGER.trace("Removed completed handler: {}", h);
        }
    }

    private void addHandler(Handler handler)
    {
        synchronized(_handlersLock)
        {
            _handlers.add(handler);
            LOGGER.trace("Added handler: {}", handler);
        }
    }

    private Handler getFirstHandler()
    {
        synchronized(_handlersLock)
        {
            if(_handlers.isEmpty())
            {
                return null;
            }
            return _handlers.get(0);
        }
    }

    private Handler getLastHandler()
    {
        synchronized(_handlersLock)
        {
            if(_handlers.isEmpty())
            {
                throw new IllegalStateException("No handlers");
            }
            return _handlers.get(_handlers.size() - 1);
        }
    }

    public void waitForAllHandlersToComplete(int timeoutMillis) throws InterruptedException
    {
        boolean countedDownOk =  waitForAllHandlersToCompleteNoAssert(timeoutMillis);

        String message = "All handlers did not complete within the " + timeoutMillis + "ms timeout.";
        Throwable t = getThrowable();
        if(t != null){
            message += System.lineSeparator() + "A *potential* reason, peer caught throwable: " + t;
        }

        assertTrue(countedDownOk, message);
    }

    public boolean waitForAllHandlersToCompleteNoAssert(int timeoutMillis) throws InterruptedException
    {
        synchronized(_handlersLock)
        {
            _handlersCompletedLatch = new CountDownLatch(_handlers.size());
        }

        return _handlersCompletedLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    void sendHeader(byte[] header)
    {
        LOGGER.debug("About to send header: {}", new Binary(header));
        _driverRunnable.sendBytes(header);
    }

    public void sendPreemptiveServerAmqpHeader() {
        // Arrange to send the AMQP header after the previous handler
        CompositeAmqpPeerRunnable comp = insertCompsiteActionForLastHandler();
        comp.add(new AmqpPeerRunnable() {
            @Override
            public void run() {
                sendHeader(AmqpHeader.HEADER);
            }
        });
    }

    public void sendEmptyFrame(boolean deferWrite)
    {
        sendFrame(FrameType.AMQP, 0, null, null, deferWrite, 0);
    }

    void sendFrame(FrameType type, int channel, DescribedType frameDescribedType, Binary framePayload, boolean deferWrite, long sendDelay)
    {
        if(channel < 0)
        {
            throw new IllegalArgumentException("Frame must be sent on a channel >= 0");
        }

        LOGGER.debug("About to send: {}", frameDescribedType);

        if(sendDelay > 0)
        {
            LOGGER.debug("Delaying send by {} ms", sendDelay);
            try {
                Thread.sleep(sendDelay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted before send", e);
            }
        }

        byte[] output = AmqpDataFramer.encodeFrame(type, channel, frameDescribedType, framePayload);

        if(deferWrite && _deferredBytes == null)
        {
            _deferredBytes = output;
        }
        else if(_deferredBytes != null)
        {
            int newCapacity = _deferredBytes.length + output.length;
            //TODO: check overflow

            byte[] newOutput = new byte[newCapacity];
            System.arraycopy(_deferredBytes, 0, newOutput, 0, _deferredBytes.length);
            System.arraycopy(output, 0, newOutput, _deferredBytes.length, output.length);

            _deferredBytes = newOutput;
            output = newOutput;
        }

        if(deferWrite)
        {
            LOGGER.debug("Deferring write until pipelined with future frame bytes");
            return;
        }
        else
        {
            //clear the deferred bytes to avoid corrupting future sends
            _deferredBytes = null;

            _driverRunnable.sendBytes(output);
        }
    }

    private OpenFrame createOpenFrame()
    {
        OpenFrame openFrame = new OpenFrame();
        openFrame.setContainerId("test-amqp-peer-container-id");
        if(advertisedIdleTimeout != 0)
        {
            openFrame.setIdleTimeOut(UnsignedInteger.valueOf(advertisedIdleTimeout));
        }

        return openFrame;
    }

    public void expectHeader(byte[] header, byte[] response)
    {
        addHandler(new HeaderHandlerImpl(header, response));
    }

    private void expectSaslAuthentication(Symbol mechanism, Matcher<Binary> initialResponseMatcher, Matcher<?> hostnameMatcher,
                                          boolean sendSaslHeaderResponse, boolean amqpHeaderSentPreemptively)
    {
        SaslMechanismsFrame saslMechanismsFrame = new SaslMechanismsFrame().setSaslServerMechanisms(mechanism);
        byte[] saslHeaderResponse = null;
        if(sendSaslHeaderResponse) {
            saslHeaderResponse = AmqpHeader.SASL_HEADER;
        }
        addHandler(new HeaderHandlerImpl(AmqpHeader.SASL_HEADER, saslHeaderResponse,
                                            new FrameSender(
                                                    this, FrameType.SASL, 0,
                                                    saslMechanismsFrame, null)));

        SaslInitMatcher saslInitMatcher = new SaslInitMatcher()
            .withMechanism(equalTo(mechanism))
            .withInitialResponse(initialResponseMatcher)
            .onCompletion(new AmqpPeerRunnable()
            {
                @Override
                public void run()
                {
                    TestAmqpPeer.this.sendFrame(
                            FrameType.SASL, 0,
                            new SaslOutcomeFrame().setCode(SASL_OK),
                            null,
                            false, 0);

                    // Now that we processed the SASL layer AMQP header, reset the
                    // peer to expect the non-SASL AMQP header.
                    _driverRunnable.expectHeader();
                }
            });

        if(hostnameMatcher != null)
        {
            saslInitMatcher.withHostname(hostnameMatcher);
        }

        addHandler(saslInitMatcher);

        if (!amqpHeaderSentPreemptively)
        {
            addHandler(new HeaderHandlerImpl(AmqpHeader.HEADER, AmqpHeader.HEADER));
        }
    }

    public void expectSaslGSSAPIFail() throws Exception {
        SaslMechanismsFrame saslMechanismsFrame = new SaslMechanismsFrame().setSaslServerMechanisms(GSSAPI);

        addHandler(new HeaderHandlerImpl(AmqpHeader.SASL_HEADER, AmqpHeader.SASL_HEADER,
                new FrameSender(
                        this, FrameType.SASL, 0,
                        saslMechanismsFrame, null)));
    }

    public void expectSaslGSSAPI(String serviceName, String keyTab, String clientAuthId) throws Exception {

        SaslMechanismsFrame saslMechanismsFrame = new SaslMechanismsFrame().setSaslServerMechanisms(GSSAPI);

        addHandler(new HeaderHandlerImpl(AmqpHeader.SASL_HEADER, AmqpHeader.SASL_HEADER,
                new FrameSender(
                        this, FrameType.SASL, 0,
                        saslMechanismsFrame, null)));

        // setup server gss context
        final Map<String, String> options = new HashMap<>();
        options.put("principal", serviceName);
        options.put("useKeyTab", "true");
        options.put("keyTab", keyTab);
        options.put("storeKey", "true");
        options.put("isInitiator", "false");
        Configuration loginCconfiguration = new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                return new AppConfigurationEntry[]{
                        new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                options)};
            }
        };

        LoginContext loginContext = new LoginContext("", null, null, loginCconfiguration);
        loginContext.login();
        final Subject serverSubject =loginContext.getSubject();

        LOGGER.info("saslServer subject:" + serverSubject.getPrivateCredentials());

        Map<String, ?> config = new HashMap<>();
        final CallbackHandler handler = new CallbackHandler() {
            @Override
            public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                LOGGER.info("Here with: " + Arrays.asList(callbacks));
                for (Callback callback :callbacks) {
                    if (callback instanceof AuthorizeCallback) {
                        AuthorizeCallback authorizeCallback = (AuthorizeCallback) callback;
                        authorizeCallback.setAuthorized(authorizeCallback.getAuthenticationID().equals(authorizeCallback.getAuthorizationID()));
                    }
                }
            }
        };
        final SaslServer saslServer = Subject.doAs(serverSubject, new PrivilegedExceptionAction<SaslServer>() {
            @Override
            public SaslServer run() throws Exception {
                return Sasl.createSaslServer(GSSAPI.toString(), null, null, config, handler);
            }
        });

        final SaslChallengeFrame challengeFrame1 = new SaslChallengeFrame();

        SaslInitMatcher saslInitMatcher = new SaslInitMatcher()
                .withMechanism(equalTo(GSSAPI))
                .withInitialResponse(new BaseMatcher<Binary>() {

                    @Override
                    public void describeTo(Description description) {}

                    @Override
                    public boolean matches(Object o) {
                        if (o == null) {
                            LOGGER.error("Got null initial response!");
                            return false;
                        }
                        final Binary binary = (Binary) o;
                        // validate via sasl
                        try {
                            byte[] challenge1data = Subject.doAs(serverSubject, new PrivilegedExceptionAction<byte[]>() {
                                @Override
                                public byte[] run() throws Exception {
                                    LOGGER.info("Evaluate Initial Response.. size:" + binary.getLength());
                                    return saslServer.evaluateResponse(binary.getArray());
                                }
                            });

                            LOGGER.info("Creating challenge 1.. size: " + challenge1data.length);
                            challengeFrame1.setChallenge(new Binary(challenge1data));

                        } catch (PrivilegedActionException e) {
                            LOGGER.error("Unexpected error during processing initial response", e);
                            throw new RuntimeException("Failed to eval initial response", e);
                        }
                        LOGGER.info("Complete:" + saslServer.isComplete());

                        return true;
                    }
                }).onCompletion(new AmqpPeerRunnable() {
                    @Override
                    public void run() {
                        LOGGER.info("Send challenge 1..");
                        TestAmqpPeer.this.sendFrame(
                                FrameType.SASL, 0,
                                challengeFrame1,
                                null,
                                false, 0);
                    }
                });

        AtomicBoolean succeeded = new AtomicBoolean(false);

        final SaslChallengeFrame challengeFrame2 = new SaslChallengeFrame();

        SaslResponseMatcher responseMatcher1 = new SaslResponseMatcher().withResponse(new BaseMatcher<Binary>() {
            @Override
            public void describeTo(Description description) {}

            @Override
            public boolean matches(Object o) {
                final Binary responseBinary1 = (Binary) o;
                // validate via sasl

                byte[] challenge2data = null;
                try {
                    challenge2data = Subject.doAs(serverSubject, new PrivilegedExceptionAction<byte[]>() {
                        @Override
                        public byte[] run() throws Exception {
                            LOGGER.info("Evaluate challenge response 1.. size:" + responseBinary1.getLength());
                            return saslServer.evaluateResponse(responseBinary1.getArray());
                        }
                    });
                } catch (PrivilegedActionException e) {
                    LOGGER.error("Unexpected error during processing challenge response 1", e);
                    throw new RuntimeException("failed to evaluate challenge response 1", e);
                }

                LOGGER.info("Creating challenge 2.. size: " + challenge2data.length);
                challengeFrame2.setChallenge(new Binary(challenge2data));

                LOGGER.info("Complete:" + saslServer.isComplete());

                return true;
            }
        }).onCompletion(new AmqpPeerRunnable() {
            @Override
            public void run() {
                LOGGER.info("Send challenge 2..");
                TestAmqpPeer.this.sendFrame(
                        FrameType.SASL, 0,
                        challengeFrame2,
                        null,
                        false, 0);
            }
        });

        SaslResponseMatcher responseMatcher2 = new SaslResponseMatcher().withResponse(new BaseMatcher<Binary>() {
            @Override
            public void describeTo(Description description) {}

            @Override
            public boolean matches(Object o) {
                final Binary binary = (Binary) o;
                // validate via sasl

                byte[] additionalData = null;
                try {
                    additionalData = Subject.doAs(serverSubject, new PrivilegedExceptionAction<byte[]>() {
                        @Override
                        public byte[] run() throws Exception {
                            LOGGER.info("Evaluate challenge response 2.. size:" + binary.getLength());
                            return saslServer.evaluateResponse(binary.getArray());
                        }
                    });
                } catch (PrivilegedActionException e) {
                    LOGGER.error("Unexpected error during processing challenge response 2", e);
                    throw new RuntimeException("failed to evaluate challenge response 2", e);
                }

                boolean complete = saslServer.isComplete();
                boolean expectedAuthId = false;
                if(complete) {
                    expectedAuthId = clientAuthId.equals(saslServer.getAuthorizationID());
                    LOGGER.info("Authorized ID: " + saslServer.getAuthorizationID());
                }

                LOGGER.info("Complete:" + complete + ", expectedAuthID:" + expectedAuthId +", additionalData:" + Arrays.toString(additionalData));

                if(complete && expectedAuthId && additionalData == null) {
                    succeeded.set(true);
                    return true;
                } else {
                    return false;
                }
            }
        }).onCompletion(new AmqpPeerRunnable() {
            @Override
            public void run() {
                SaslOutcomeFrame saslOutcome = new SaslOutcomeFrame();
                if (saslServer.isComplete() && succeeded.get()) {
                    saslOutcome.setCode(SASL_OK);
                } else {
                    saslOutcome.setCode(SASL_FAIL_AUTH);
                }

                LOGGER.info("Send Outcome");
                TestAmqpPeer.this.sendFrame(
                        FrameType.SASL, 0,
                        saslOutcome,
                        null,
                        false, 0);

                // Now that we processed the SASL layer AMQP header, reset the
                // peer to expect the non-SASL AMQP header.
                _driverRunnable.expectHeader();
            }
        });

        addHandler(saslInitMatcher);
        addHandler(responseMatcher1);
        addHandler(responseMatcher2);
        addHandler(new HeaderHandlerImpl(AmqpHeader.HEADER, AmqpHeader.HEADER));
    }

    public void expectSaslPlain(String username, String password)
    {
        byte[] usernameBytes = username.getBytes(StandardCharsets.UTF_8);
        byte[] passwordBytes = password.getBytes(StandardCharsets.UTF_8);
        byte[] data = new byte[usernameBytes.length+passwordBytes.length+2];
        System.arraycopy(usernameBytes, 0, data, 1, usernameBytes.length);
        System.arraycopy(passwordBytes, 0, data, 2 + usernameBytes.length, passwordBytes.length);

        Matcher<Binary> initialResponseMatcher = equalTo(new Binary(data));

        expectSaslAuthentication(PLAIN, initialResponseMatcher, null, true, false);
    }

    public void expectSaslXOauth2(String username, String password)
    {
        byte[] usernameBytes = username.getBytes(StandardCharsets.UTF_8);
        byte[] passwordBytes = password.getBytes(StandardCharsets.UTF_8);
        byte[] data = new byte[usernameBytes.length+passwordBytes.length+20];

        System.arraycopy("user=".getBytes(StandardCharsets.US_ASCII), 0, data, 0, 5);
        System.arraycopy(usernameBytes, 0, data, 5, usernameBytes.length);
        data[5+usernameBytes.length] = 1;
        System.arraycopy("auth=Bearer ".getBytes(StandardCharsets.US_ASCII), 0, data, 6+usernameBytes.length, 12);
        System.arraycopy(passwordBytes, 0, data, 18 + usernameBytes.length, passwordBytes.length);
        data[data.length-2] = 1;
        data[data.length-1] = 1;

        Matcher<Binary> initialResponseMatcher = equalTo(new Binary(data));

        expectSaslAuthentication(XOAUTH2, initialResponseMatcher, null, true, false);
    }

    public void expectSaslExternal()
    {
        if(!_driverRunnable.isNeedClientCert())
        {
            throw new IllegalStateException("need-client-cert must be enabled on the test peer");
        }

        expectSaslAuthentication(EXTERNAL, equalTo(new Binary(new byte[0])), null, true, false);
    }

    public void expectSaslAnonymous()
    {
        expectSaslAnonymous(null);
    }

    public void expectSaslAnonymous(Matcher<?> hostnameMatcher)
    {
        expectSaslAuthentication(ANONYMOUS, equalTo(new Binary(new byte[0])), hostnameMatcher, true, false);
    }

    public void expectSaslAnonymousWithPreEmptiveServerHeader()
    {
        assertThat("Peer should be created with instruction to send preemptively", _driverRunnable.isSendSaslHeaderPreEmptively(), equalTo(true));
        boolean sendSaslHeaderResponse = false; // Must arrange for the server to have already sent it preemptively
        expectSaslAuthentication(ANONYMOUS, equalTo(new Binary(new byte[0])), null, sendSaslHeaderResponse, false);
    }

    public void expectSaslAnonymousWithServerAmqpHeaderSentPreemptively()
    {
        expectSaslAuthentication(ANONYMOUS, equalTo(new Binary(new byte[0])), null, true, true);
    }

    public void expectSaslFailingAuthentication(Symbol[] serverMechs, Symbol clientSelectedMech)
    {
        expectSaslFailingExchange(serverMechs, clientSelectedMech, SASL_FAIL_AUTH);
    }

    public void expectSaslFailingExchange(Symbol[] serverMechs, Symbol clientSelectedMech, UnsignedByte saslFailureAuthCode)
    {
        SaslMechanismsFrame saslMechanismsFrame = new SaslMechanismsFrame().setSaslServerMechanisms(serverMechs);
        addHandler(new HeaderHandlerImpl(AmqpHeader.SASL_HEADER, AmqpHeader.SASL_HEADER,
                                            new FrameSender(
                                                    this, FrameType.SASL, 0,
                                                    saslMechanismsFrame, null)));

        if(saslFailureAuthCode.compareTo(SASL_FAIL_AUTH) < 0 || saslFailureAuthCode.compareTo(SASL_SYS_TEMP) > 0) {
            throw new IllegalArgumentException("A valid failing SASL code must be supplied");
        }

        SaslInitMatcher saslInitMatcher = new SaslInitMatcher().withMechanism(equalTo(clientSelectedMech));
        saslInitMatcher.onCompletion(new AmqpPeerRunnable()
        {
            @Override
            public void run()
            {
                TestAmqpPeer.this.sendFrame(
                        FrameType.SASL, 0,
                        new SaslOutcomeFrame().setCode(saslFailureAuthCode),
                        null,
                        false, 0);
                _driverRunnable.expectHeader();
            }
        });
        addHandler(saslInitMatcher);
    }

    public void expectSaslMechanismNegotiationFailure(Symbol[] serverMechs)
    {
        SaslMechanismsFrame saslMechanismsFrame = new SaslMechanismsFrame().setSaslServerMechanisms(serverMechs);
        FrameSender mechanismsFrameSender = new FrameSender(this, FrameType.SASL, 0, saslMechanismsFrame, null);

        addHandler(new HeaderHandlerImpl(AmqpHeader.SASL_HEADER, AmqpHeader.SASL_HEADER, mechanismsFrameSender));
    }

    /**
     * Expect a connection that does not use a SASL layer, but proceeds straight
     * to the AMQP connection (useful to skip a stage for connections that don't
     * require SASL, e.g. because of anonymous or client certificate authentication).
     *
     * @param maxFrameSizeMatcher
     *      The Matcher used to validate the maxFrameSize setting.
     */
    public void expectSaslLayerDisabledConnect(Matcher<?> maxFrameSizeMatcher)
    {
        addHandler(new HeaderHandlerImpl(AmqpHeader.HEADER, AmqpHeader.HEADER));

        OpenFrame openFrame = createOpenFrame();

        OpenMatcher openMatcher = new OpenMatcher()
            .withContainerId(notNullValue(String.class));

        if(maxFrameSizeMatcher != null) {
            openMatcher.withMaxFrameSize(maxFrameSizeMatcher);
        }

        openMatcher.onCompletion(new FrameSender(
                    this, FrameType.AMQP, 0,
                    openFrame,
                    null));

        addHandler(openMatcher);
    }

    public void expectOpen() {
        expectOpen(false);
    }

    public void expectOpen(boolean deferOpened) {
        expectOpen(null, null, deferOpened);
    }

    public void expectOpen(Map<Symbol, Object> serverProperties) {
        expectOpen(DEFAULT_DESIRED_CAPABILITIES, new Symbol[] { AmqpSupport.SOLE_CONNECTION_CAPABILITY }, null, serverProperties, null, null, false);
    }

    public void expectOpen(Map<Symbol, Object> serverProperties, Symbol[] serverCapabilities) {
        expectOpen(DEFAULT_DESIRED_CAPABILITIES, serverCapabilities, null, serverProperties, null, null, false);
    }

    public void expectOpen(Matcher<?> clientPropertiesMatcher, Matcher<?> hostnameMatcher, boolean deferOpened) {
        expectOpen(clientPropertiesMatcher, nullValue(), hostnameMatcher, deferOpened);
    }

    public void expectOpen(Matcher<?> clientPropertiesMatcher, Matcher<?> idleTimeoutMatcher, Matcher<?> hostnameMatcher, boolean deferOpened) {
        expectOpen(DEFAULT_DESIRED_CAPABILITIES, new Symbol[] { AmqpSupport.SOLE_CONNECTION_CAPABILITY }, clientPropertiesMatcher, null, null, hostnameMatcher, deferOpened);
    }

    public void sendPreemptiveServerOpenFrame() {
        sendOpenFrameAfterLastAction(false, null);
    }

    public void sendOpenFrameAfterLastAction(boolean deferWrite, Map<Symbol, Object> serverProperties) {
        // Arrange to send the Open frame after the previous handler
        OpenFrame open = createOpenFrame();
        if (serverProperties != null) {
            open.setProperties(serverProperties);
        }

        CompositeAmqpPeerRunnable comp = insertCompsiteActionForLastHandler();

        FrameSender openSender = new FrameSender(this, FrameType.AMQP, 0, open, null);
        if (deferWrite) {
            openSender.setDeferWrite(true);
        }

        comp.add(openSender);
    }

    public void expectOpen(Symbol[] desiredCapabilities, Symbol[] serverCapabilities,
                           Matcher<?> clientPropertiesMatcher, Map<Symbol, Object> serverProperties,
                           Matcher<?> idleTimeoutMatcher, Matcher<?> hostnameMatcher, boolean deferOpened) {

        OpenFrame open = createOpenFrame();
        if (serverCapabilities != null) {
            open.setOfferedCapabilities(serverCapabilities);
        }

        if (serverProperties != null) {
            open.setProperties(serverProperties);
        }

        OpenMatcher openMatcher = new OpenMatcher().withContainerId(notNullValue(String.class));
        if (!deferOpened) {
            openMatcher.onCompletion(new FrameSender(this, FrameType.AMQP, 0, open, null));
        }

        if (desiredCapabilities != null) {
            openMatcher.withDesiredCapabilities(arrayContaining(desiredCapabilities));
        } else {
            openMatcher.withDesiredCapabilities(nullValue());
        }

        if (idleTimeoutMatcher != null) {
            openMatcher.withIdleTimeOut(idleTimeoutMatcher);
        }

        if (hostnameMatcher != null) {
            openMatcher.withHostname(hostnameMatcher);
        }

        if (clientPropertiesMatcher != null) {
            openMatcher.withProperties(clientPropertiesMatcher);
        }

        addHandler(openMatcher);
    }

    public void rejectConnect(Symbol errorType, String errorMessage, Map<Symbol, Object> errorInfo) {
        // Expect a connection, establish through the SASL negotiation and sending of the Open frame
        Map<Symbol, Object> serverProperties = new HashMap<Symbol, Object>();
        serverProperties.put(AmqpSupport.CONNECTION_OPEN_FAILED, true);

        expectSaslAnonymous();
        expectOpen(serverProperties);

        // Now generate the Close frame with the supplied error
        final FrameSender closeSender = createCloseFrameSender(errorType, errorMessage, errorInfo, 0);

        // Update the handler to send the Close frame after the Open frame.
        CompositeAmqpPeerRunnable comp = insertCompsiteActionForLastHandler();
        comp.add(closeSender);

        addHandler(new CloseMatcher().withError(Matchers.nullValue()));
    }

    public void expectClose()
    {
        expectClose(Matchers.nullValue(), true);
    }

    public void expectClose(boolean sendReply)
    {
        expectClose(Matchers.nullValue(), sendReply);
    }

    public void expectClose(Matcher<?> errorMatcher, boolean sendReply)
    {
        CloseMatcher closeMatcher = new CloseMatcher().withError(errorMatcher);
        if(sendReply) {
            closeMatcher.onCompletion(new FrameSender(this, FrameType.AMQP, 0,
                    new CloseFrame(),
                    null));
        }

        addHandler(closeMatcher);
    }

    public void expectBegin()
    {
        expectBegin(notNullValue(), true);
    }

    public void expectBegin(boolean sendResponse)
    {
        expectBegin(notNullValue(), sendResponse);
    }

    public void expectBegin(Matcher<?> outgoingWindowMatcher, boolean sendResponse)
    {
        final BeginMatcher beginMatcher = new BeginMatcher()
                .withRemoteChannel(nullValue())
                .withNextOutgoingId(equalTo(UnsignedInteger.ONE))
                .withIncomingWindow(notNullValue());
        if(outgoingWindowMatcher != null)
        {
            beginMatcher.withOutgoingWindow(notNullValue());
        }
        else
        {
            beginMatcher.withOutgoingWindow(outgoingWindowMatcher);
        }

        if(sendResponse) {
            // The response will have its remoteChannel field dynamically set based on incoming value
            final BeginFrame beginResponse = new BeginFrame()
            .setNextOutgoingId(UnsignedInteger.ONE)
            .setIncomingWindow(UnsignedInteger.ZERO)
            .setOutgoingWindow(UnsignedInteger.ZERO);

            // The response frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
            final FrameSender beginResponseSender = new FrameSender(this, FrameType.AMQP, -1, beginResponse, null);
            beginResponseSender.setValueProvider(new ValueProvider()
            {
                @Override
                public void setValues()
                {
                    int actualChannel = beginMatcher.getActualChannel();

                    beginResponseSender.setChannel(actualChannel);
                    beginResponse.setRemoteChannel(
                            UnsignedShort.valueOf((short) actualChannel));

                    _lastInitiatedChannel = actualChannel;
                }
            });
            beginMatcher.onCompletion(beginResponseSender);
        }

        addHandler(beginMatcher);
    }

    public void expectEnd()
    {
        expectEnd(true);
    }

    public void expectEnd(boolean sendResponse)
    {
        final EndMatcher endMatcher = new EndMatcher();

        if (sendResponse) {
            final EndFrame endResponse = new EndFrame();

            // The response frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
            final FrameSender frameSender = new FrameSender(this, FrameType.AMQP, -1, endResponse, null);
            frameSender.setValueProvider(new ValueProvider()
            {
                @Override
                public void setValues()
                {
                    frameSender.setChannel(endMatcher.getActualChannel());
                }
            });
            endMatcher.onCompletion(frameSender);
        }

        addHandler(endMatcher);
    }

    public void expectTempQueueCreationAttach(final String dynamicAddress)
    {
        expectTempNodeCreationAttach(dynamicAddress, AmqpDestinationHelper.TEMP_QUEUE_CAPABILITY, false, false, null, null);
    }

    public void expectTempQueueCreationAttach(final String dynamicAddress, boolean sendReponse)
    {
        expectTempNodeCreationAttach(dynamicAddress, AmqpDestinationHelper.TEMP_QUEUE_CAPABILITY, sendReponse, false, false, null, null);
    }

    public void expectTempTopicCreationAttach(final String dynamicAddress)
    {
        expectTempNodeCreationAttach(dynamicAddress, AmqpDestinationHelper.TEMP_TOPIC_CAPABILITY, false, false, null, null);
    }

    public void expectTempTopicCreationAttach(final String dynamicAddress, boolean sendReponse)
    {
        expectTempNodeCreationAttach(dynamicAddress, AmqpDestinationHelper.TEMP_TOPIC_CAPABILITY, sendReponse, false, false, null, null);
    }

    public void expectAndRefuseTempQueueCreationAttach(Symbol errorType, String errorMessage, boolean deferAttachResponseWrite)
    {
        expectTempNodeCreationAttach(null, AmqpDestinationHelper.TEMP_QUEUE_CAPABILITY, true, deferAttachResponseWrite, errorType, errorMessage);
    }

    public void expectAndRefuseTempTopicCreationAttach(Symbol errorType, String errorMessage, boolean deferAttachResponseWrite)
    {
        expectTempNodeCreationAttach(null, AmqpDestinationHelper.TEMP_TOPIC_CAPABILITY, true, deferAttachResponseWrite, errorType, errorMessage);
    }

    private void expectTempNodeCreationAttach(final String dynamicAddress, final Symbol nodeTypeCapability, final boolean refuseLink, boolean deferAttachResponseWrite, Symbol errorType, String errorMessage)
    {
        expectTempNodeCreationAttach(dynamicAddress, nodeTypeCapability, true, refuseLink, deferAttachResponseWrite, errorType, errorMessage);
    }

    private void expectTempNodeCreationAttach(final String dynamicAddress, final Symbol nodeTypeCapability, boolean sendResponse, final boolean refuseLink, boolean deferAttachResponseWrite, Symbol errorType, String errorMessage)
    {
        TargetMatcher targetMatcher = new TargetMatcher();
        targetMatcher.withAddress(nullValue());
        targetMatcher.withDynamic(equalTo(true));
        targetMatcher.withDurable(equalTo(TerminusDurability.NONE));
        targetMatcher.withExpiryPolicy(equalTo(TerminusExpiryPolicy.LINK_DETACH));
        targetMatcher.withDynamicNodeProperties(hasEntry(equalTo(DYNAMIC_NODE_LIFETIME_POLICY), new DeleteOnCloseMatcher()));
        targetMatcher.withCapabilities(arrayContaining(nodeTypeCapability));

        final AttachMatcher attachMatcher = new AttachMatcher()
                .withName(notNullValue())
                .withHandle(notNullValue())
                .withRole(equalTo(Role.SENDER))
                .withSndSettleMode(equalTo(SenderSettleMode.UNSETTLED))
                .withRcvSettleMode(equalTo(ReceiverSettleMode.FIRST))
                .withSource(notNullValue())
                .withTarget(targetMatcher);

        if (sendResponse)
        {
            final AttachFrame attachResponse = new AttachFrame()
                                .setRole(Role.RECEIVER)
                                .setSndSettleMode(SenderSettleMode.UNSETTLED)
                                .setRcvSettleMode(ReceiverSettleMode.FIRST);

            // The response frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
            final FrameSender attachResponseSender = new FrameSender(this, FrameType.AMQP, -1, attachResponse, null);
            attachResponseSender.setValueProvider(new ValueProvider()
            {
                @Override
                public void setValues()
                {
                    Object receivedHandle = attachMatcher.getReceivedHandle();

                    attachResponseSender.setChannel(attachMatcher.getActualChannel());
                    attachResponse.setHandle(receivedHandle);
                    attachResponse.setName(attachMatcher.getReceivedName());
                    attachResponse.setSource(attachMatcher.getReceivedSource());

                    if (!refuseLink) {
                        Target t = (Target) createTargetObjectFromDescribedType(attachMatcher.getReceivedTarget());
                        t.setAddress(dynamicAddress);
                        attachResponse.setTarget(t);
                    } else {
                        attachResponse.setTarget(null);
                    }

                    _lastInitiatedLinkHandle = (UnsignedInteger) receivedHandle;
                }
            });

            if (deferAttachResponseWrite)
            {
                // Defer writing the attach frame until the subsequent frame is also ready
                attachResponseSender.setDeferWrite(true);
            }

            CompositeAmqpPeerRunnable composite = new CompositeAmqpPeerRunnable();
            composite.add(attachResponseSender);

            if (!refuseLink) {
                final FlowFrame flowFrame = new FlowFrame().setNextIncomingId(UnsignedInteger.ONE)  //TODO: shouldnt be hard coded
                        .setIncomingWindow(UnsignedInteger.valueOf(2048))
                        .setNextOutgoingId(UnsignedInteger.ONE) //TODO: shouldn't be hard coded
                        .setOutgoingWindow(UnsignedInteger.valueOf(2048))
                        .setLinkCredit(UnsignedInteger.valueOf(100));

                // The flow frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
                final FrameSender flowFrameSender = new FrameSender(this, FrameType.AMQP, -1, flowFrame, null);
                flowFrameSender.setValueProvider(new ValueProvider()
                {
                    @Override
                    public void setValues()
                    {
                        flowFrameSender.setChannel(attachMatcher.getActualChannel());
                        flowFrame.setHandle(attachMatcher.getReceivedHandle());
                        flowFrame.setDeliveryCount(attachMatcher.getReceivedInitialDeliveryCount());
                    }
                });

                composite.add(flowFrameSender);
            } else {
                final DetachFrame detachResponse = new DetachFrame().setClosed(true);
                if (errorType != null)
                {
                    org.apache.qpid.jms.test.testpeer.describedtypes.Error detachError = new org.apache.qpid.jms.test.testpeer.describedtypes.Error();

                    detachError.setCondition(errorType);
                    detachError.setDescription(errorMessage);

                    detachResponse.setError(detachError);
                }

                // The response frame channel will be dynamically set based on the
                // incoming frame. Using the -1 is an illegal placeholder.
                final FrameSender detachResonseSender = new FrameSender(this, FrameType.AMQP, -1, detachResponse, null);
                detachResonseSender.setValueProvider(new ValueProvider() {
                     @Override
                     public void setValues() {
                          detachResonseSender.setChannel(attachMatcher.getActualChannel());
                          detachResponse.setHandle(attachMatcher.getReceivedHandle());
                     }
                });

                composite.add(detachResonseSender);
            }

            attachMatcher.onCompletion(composite);
        }

        addHandler(attachMatcher);
    }

    public void expectSenderAttachButDoNotRespond()
    {
        final AttachMatcher attachMatcher = new AttachMatcher()
                .withName(notNullValue())
                .withHandle(notNullValue())
                .withRole(equalTo(Role.SENDER))
                .withSndSettleMode(Matchers.oneOf(SenderSettleMode.SETTLED, SenderSettleMode.UNSETTLED))
                .withRcvSettleMode(equalTo(ReceiverSettleMode.FIRST))
                .withSource(notNullValue())
                .withTarget(notNullValue());

        addHandler(attachMatcher);
    }

    public void expectSenderAttach()
    {
        expectSenderAttach(notNullValue(), false, false);
    }

    public void expectSettledSenderAttach()
    {
        expectSenderAttach(notNullValue(), notNullValue(), true, false, false, false, 0, DEFAULT_PRODUCER_CREDIT, null, null);
    }

    public void expectSenderAttachWithoutGrantingCredit()
    {
        expectSenderAttach(notNullValue(), notNullValue(), false, false, false, 0, 0, null, null);
    }

    public void expectSenderAttach(long creditFlowDelay)
    {
        expectSenderAttach(notNullValue(), notNullValue(), false, false, false, creditFlowDelay, DEFAULT_PRODUCER_CREDIT, null, null);
    }

    public void expectSenderAttach(final Matcher<?> targetMatcher, final boolean refuseLink, boolean deferAttachResponseWrite)
    {
        expectSenderAttach(notNullValue(), targetMatcher, refuseLink, deferAttachResponseWrite);
    }

    public void expectSenderAttach(final Matcher<?> sourceMatcher, final Matcher<?> targetMatcher, final boolean refuseLink, boolean deferAttachResponseWrite)
    {
        expectSenderAttach(sourceMatcher, targetMatcher, refuseLink, false, deferAttachResponseWrite, 0, null, null);
    }

    public void expectSenderAttach(final Matcher<?> sourceMatcher, final Matcher<?> targetMatcher, final boolean refuseLink, boolean omitDetach, boolean deferAttachResponseWrite, long creditFlowDelay, Symbol errorType, String errorMessage)
    {
        expectSenderAttach(sourceMatcher, targetMatcher, refuseLink, omitDetach, deferAttachResponseWrite, creditFlowDelay, DEFAULT_PRODUCER_CREDIT, errorType, errorMessage);
    }

    public void expectSenderAttach(final Matcher<?> sourceMatcher, final Matcher<?> targetMatcher, final boolean refuseLink, boolean omitDetach, boolean deferAttachResponseWrite, long creditFlowDelay, int creditAmount, Symbol errorType, String errorMessage)
    {
        expectSenderAttach(sourceMatcher, targetMatcher, false, refuseLink, omitDetach, deferAttachResponseWrite, creditFlowDelay, creditAmount, errorType, errorMessage);
    }

    public void expectSenderAttach(final Matcher<?> sourceMatcher, final Matcher<?> targetMatcher, final boolean senderSettled, final boolean refuseLink, boolean omitDetach, boolean deferAttachResponseWrite, long creditFlowDelay, int creditAmount, Symbol errorType, String errorMessage)
    {
        expectSenderAttach(sourceMatcher, targetMatcher, senderSettled, refuseLink, omitDetach, deferAttachResponseWrite, creditFlowDelay, creditAmount, errorType, errorMessage, null, null);
    }


    public void expectSenderAttach(final Matcher<?> sourceMatcher, final Matcher<?> targetMatcher, final boolean senderSettled,
                                   final boolean refuseLink, boolean omitDetach, boolean deferAttachResponseWrite,
                                   long creditFlowDelay, int creditAmount, Symbol errorType, String errorMessage,
                                   Matcher<?> desiredCapabilitiesMatcher, Symbol[] offeredCapabilitiesResponse)
    {
        expectSenderAttach(sourceMatcher, targetMatcher, senderSettled, refuseLink, omitDetach, deferAttachResponseWrite,
                           creditFlowDelay, creditAmount, errorType, errorMessage, desiredCapabilitiesMatcher,
                           offeredCapabilitiesResponse, null, null);
    }

    public void expectSenderAttach(final Matcher<?> sourceMatcher, final Matcher<?> targetMatcher, final boolean senderSettled,
                                   final boolean refuseLink, boolean omitDetach, boolean deferAttachResponseWrite,
                                   long creditFlowDelay, int creditAmount, Symbol errorType, String errorMessage,
                                   Matcher<?> desiredCapabilitiesMatcher, Symbol[] offeredCapabilitiesResponse,
                                   Matcher<?> propertiesMatcher, Map<Symbol, Object> propertiesResponse)
    {
        final AttachMatcher attachMatcher = new AttachMatcher()
                .withName(notNullValue())
                .withHandle(notNullValue())
                .withRole(equalTo(Role.SENDER))
                .withSndSettleMode(equalTo(senderSettled ? SenderSettleMode.SETTLED : SenderSettleMode.UNSETTLED))
                .withRcvSettleMode(equalTo(ReceiverSettleMode.FIRST))
                .withSource(sourceMatcher)
                .withTarget(targetMatcher);

        if(desiredCapabilitiesMatcher != null) {
            attachMatcher.withDesiredCapabilities(desiredCapabilitiesMatcher);
        }

        if(propertiesMatcher != null) {
            attachMatcher.withProperties(propertiesMatcher);
        }

        final AttachFrame attachResponse = new AttachFrame()
                            .setRole(Role.RECEIVER)
                            .setOfferedCapabilities(offeredCapabilitiesResponse)
                            .setSndSettleMode(senderSettled ? SenderSettleMode.SETTLED : SenderSettleMode.UNSETTLED)
                            .setRcvSettleMode(ReceiverSettleMode.FIRST)
                            .setProperties(propertiesResponse);

        expectSenderAttach(attachMatcher, attachResponse, refuseLink, omitDetach, deferAttachResponseWrite, creditFlowDelay, creditAmount, errorType, errorMessage);
    }

    public void expectSenderAttach(final AttachMatcher attachMatcher, final AttachFrame attachResponse, final boolean refuseLink, boolean omitDetach, boolean deferAttachResponseWrite, long creditFlowDelay, int creditAmount, Symbol errorType, String errorMessage)
    {
        // The response frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
        final FrameSender attachResponseSender = new FrameSender(this, FrameType.AMQP, -1, attachResponse, null);
        attachResponseSender.setValueProvider(new ValueProvider()
        {
            @Override
            public void setValues()
            {
                Object receivedHandle = attachMatcher.getReceivedHandle();

                attachResponseSender.setChannel(attachMatcher.getActualChannel());
                attachResponse.setHandle(receivedHandle);
                attachResponse.setName(attachMatcher.getReceivedName());
                attachResponse.setSource(createSourceObjectFromDescribedType(attachMatcher.getReceivedSource()));
                if(refuseLink) {
                    attachResponse.setTarget(null);
                } else {
                    attachResponse.setTarget(createTargetObjectFromDescribedType(attachMatcher.getReceivedTarget()));
                }

                _lastInitiatedLinkHandle = (UnsignedInteger) receivedHandle;

                Object target = createTargetObjectFromDescribedType(attachMatcher.getReceivedTarget());
                if (target instanceof Coordinator)
                {
                    _lastInitiatedCoordinatorLinkHandle = (UnsignedInteger) receivedHandle;
                }
            }
        });

        if(deferAttachResponseWrite)
        {
            // Defer writing the attach frame until the subsequent frame is also ready
            attachResponseSender.setDeferWrite(true);
        }

        CompositeAmqpPeerRunnable composite = new CompositeAmqpPeerRunnable();
        composite.add(attachResponseSender);
        if (refuseLink) {
            if (!omitDetach) {
                final DetachFrame detachResponse = new DetachFrame().setClosed(true);
                if (errorType != null)
                {
                    org.apache.qpid.jms.test.testpeer.describedtypes.Error detachError = new org.apache.qpid.jms.test.testpeer.describedtypes.Error();

                    detachError.setCondition(errorType);
                    detachError.setDescription(errorMessage);

                    detachResponse.setError(detachError);
                }

                // The response frame channel will be dynamically set based on the
                // incoming frame. Using the -1 is an illegal placeholder.
                final FrameSender detachResonseSender = new FrameSender(this, FrameType.AMQP, -1, detachResponse, null);
                detachResonseSender.setValueProvider(new ValueProvider() {
                     @Override
                     public void setValues() {
                          detachResonseSender.setChannel(attachMatcher.getActualChannel());
                          detachResponse.setHandle(attachMatcher.getReceivedHandle());
                     }
                });

                composite.add(detachResonseSender);
            }
        } else {
            final FlowFrame flowFrame = new FlowFrame().setNextIncomingId(UnsignedInteger.ONE) //TODO: shouldnt be hard coded
                .setIncomingWindow(UnsignedInteger.valueOf(2048))
                .setNextOutgoingId(UnsignedInteger.ONE) //TODO: shouldnt be hard coded
                .setOutgoingWindow(UnsignedInteger.valueOf(2048))
                .setLinkCredit(UnsignedInteger.valueOf(creditAmount));

            // The flow frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
            final FrameSender flowFrameSender = new FrameSender(this, FrameType.AMQP, -1, flowFrame, null);
            flowFrameSender.setValueProvider(new ValueProvider()
            {
                @Override
                public void setValues()
                {
                    flowFrameSender.setChannel(attachMatcher.getActualChannel());
                    flowFrame.setHandle(attachMatcher.getReceivedHandle());
                    flowFrame.setDeliveryCount(attachMatcher.getReceivedInitialDeliveryCount());
                }
            });

            flowFrameSender.setSendDelay(creditFlowDelay);

            composite.add(flowFrameSender);
        }

        attachMatcher.onCompletion(composite);

        addHandler(attachMatcher);
    }

    public void expectCoordinatorAttach()
    {
        expectCoordinatorAttach(false, false, null, null);
    }

    public void expectCoordinatorAttach(SourceMatcher sourceMatcher)
    {
        expectCoordinatorAttach(sourceMatcher, false, false, null, null);
    }

    public void expectCoordinatorAttach(boolean refuseLink, boolean deferAttachResponseWrite)
    {
        expectCoordinatorAttach(refuseLink, deferAttachResponseWrite, null, null);
    }

    public void expectCoordinatorAttach(final boolean refuseLink, boolean deferAttachResponseWrite, Symbol errorType, String errorMessage)
    {
        expectCoordinatorAttach(notNullValue(), refuseLink, deferAttachResponseWrite, errorType, errorMessage);
    }

    private void expectCoordinatorAttach(Matcher<Object> sourceMatcher, final boolean refuseLink, boolean deferAttachResponseWrite, Symbol errorType, String errorMessage) {
        expectSenderAttach(sourceMatcher, new CoordinatorMatcher(), refuseLink, false, deferAttachResponseWrite, 0, errorType, errorMessage);
    }

    public void expectQueueBrowserAttach()
    {
        SourceMatcher sourceMatcher = new SourceMatcher();
        sourceMatcher.withDistributionMode(equalTo(StdDistMode.COPY));

        expectReceiverAttach(notNullValue(), sourceMatcher, true);
    }

    public void expectReceiverAttachButDoNotRespond()
    {
        final AttachMatcher attachMatcher = new AttachMatcher()
                .withName(notNullValue())
                .withHandle(notNullValue())
                .withRole(equalTo(Role.RECEIVER))
                .withSndSettleMode(Matchers.oneOf(SenderSettleMode.SETTLED, SenderSettleMode.UNSETTLED))
                .withRcvSettleMode(equalTo(ReceiverSettleMode.FIRST))
                .withSource(notNullValue())
                .withTarget(notNullValue());

        addHandler(attachMatcher);
    }

    public void expectReceiverAttach()
    {
        expectReceiverAttach(notNullValue(), notNullValue());
    }

    public void expectSettledReceiverAttach()
    {
        expectReceiverAttach(notNullValue(), notNullValue(), true, false, false, false, null, null);
    }

    public void expectReceiverAttach(final Matcher<?> linkNameMatcher, final Matcher<?> sourceMatcher)
    {
        expectReceiverAttach(linkNameMatcher, sourceMatcher, false, false, false, false, null, null);
    }

    public void expectReceiverAttach(final Matcher<?> linkNameMatcher, final Matcher<?> sourceMatcher, final boolean settled)
    {
        expectReceiverAttach(linkNameMatcher, sourceMatcher, settled, false, false, false, null, null);
    }

    public void expectReceiverAttach(final Matcher<?> linkNameMatcher, final Matcher<?> sourceMatcher, final boolean refuseLink, boolean deferAttachResponseWrite)
    {
        expectReceiverAttach(linkNameMatcher, sourceMatcher, false, refuseLink, false, deferAttachResponseWrite, null, null);
    }

    public void expectReceiverAttach(final Matcher<?> linkNameMatcher, final Matcher<?> sourceMatcher, final boolean settled, final boolean refuseLink, boolean deferAttachResponseWrite)
    {
        expectReceiverAttach(linkNameMatcher, sourceMatcher, settled, refuseLink, false, deferAttachResponseWrite, null, null);
    }

    public void expectReceiverAttach(final Matcher<?> linkNameMatcher, final Matcher<?> sourceMatcher, final boolean settled, final boolean refuseLink,
                                     boolean omitDetach, boolean deferAttachResponseWrite, Symbol errorType, String errorMessage)
    {
        expectReceiverAttach(linkNameMatcher, sourceMatcher, settled, refuseLink, omitDetach, deferAttachResponseWrite, errorType, errorMessage, null, null, null);
    }

    public void expectReceiverAttach(Matcher<?> linkNameMatcher, Matcher<?> sourceMatcher,
                                     Matcher<?> desiredCapabilitiesMatcher, Symbol[] offeredCapabilitiesResponse,
                                     Matcher<?> offeredCapabilitiesMatcher, Symbol[] desiredCapabilitiesResponse) {
        expectReceiverAttach(linkNameMatcher, sourceMatcher, false, false, false, false, null, null, null, desiredCapabilitiesMatcher, offeredCapabilitiesResponse, offeredCapabilitiesMatcher, desiredCapabilitiesResponse, null, null);
    }

    public void expectReceiverAttach(Matcher<?> linkNameMatcher, Matcher<?> sourceMatcher,
                                     Matcher<?> desiredCapabilitiesMatcher, Symbol[] offeredCapabilitiesResponse,
                                     Matcher<?> offeredCapabilitiesMatcher, Symbol[] desiredCapabilitiesResponse,
                                     Matcher<?> propertiesMatcher, Map<Symbol, Object> properties) {
        expectReceiverAttach(linkNameMatcher, sourceMatcher, false, false, false, false, null, null, null, desiredCapabilitiesMatcher, offeredCapabilitiesResponse, offeredCapabilitiesMatcher, desiredCapabilitiesResponse, propertiesMatcher, properties);
    }

    private void expectReceiverAttach(final Matcher<?> linkNameMatcher, final Matcher<?> sourceMatcher, final boolean settled, final boolean refuseLink,
            boolean omitDetach, boolean deferAttachResponseWrite, Symbol errorType, String errorMessage, final Source responseSourceOverride,
            Matcher<?> desiredCapabilitiesMatcher, Symbol[] offeredCapabilitiesResponse)
    {
        expectReceiverAttach(linkNameMatcher, sourceMatcher, settled, refuseLink, omitDetach, deferAttachResponseWrite, errorType, errorMessage, responseSourceOverride, desiredCapabilitiesMatcher, offeredCapabilitiesResponse, null, null, null, null);
    }

    private void expectReceiverAttach(final Matcher<?> linkNameMatcher, final Matcher<?> sourceMatcher, final boolean settled, final boolean refuseLink,
            boolean omitDetach, boolean deferAttachResponseWrite, Symbol errorType, String errorMessage, final Source responseSourceOverride,
            Matcher<?> desiredCapabilitiesMatcher, Symbol[] offeredCapabilitiesResponse,
            Matcher<?> offeredCapabilitiesMatcher, Symbol[] desiredCapabilitiesResponse,
            Matcher<?> propertiesMatcher, Map<Symbol, Object> properties)
    {
        final AttachMatcher attachMatcher = new AttachMatcher()
                .withName(linkNameMatcher)
                .withHandle(notNullValue())
                .withRole(equalTo(Role.RECEIVER))
                .withSndSettleMode(equalTo(settled ? SenderSettleMode.SETTLED : SenderSettleMode.UNSETTLED))
                .withRcvSettleMode(equalTo(ReceiverSettleMode.FIRST))
                .withSource(sourceMatcher)
                .withTarget(notNullValue());

        if(desiredCapabilitiesMatcher != null) {
            attachMatcher.withDesiredCapabilities(desiredCapabilitiesMatcher);
        }
        if(offeredCapabilitiesMatcher != null) {
            attachMatcher.withOfferedCapabilities(offeredCapabilitiesMatcher);
        }
        if(propertiesMatcher != null) {
            attachMatcher.withProperties(propertiesMatcher);
        }

        final AttachFrame attachResponse = new AttachFrame()
                            .setRole(Role.SENDER)
                            .setOfferedCapabilities(offeredCapabilitiesResponse)
                            .setDesiredCapabilities(desiredCapabilitiesResponse)
                            .setSndSettleMode(settled ? SenderSettleMode.SETTLED : SenderSettleMode.UNSETTLED)
                            .setRcvSettleMode(ReceiverSettleMode.FIRST)
                            .setInitialDeliveryCount(UnsignedInteger.ZERO)
                            .setProperties(properties);

        expectReceiverAttach(attachMatcher, attachResponse, refuseLink, omitDetach, deferAttachResponseWrite, errorType, errorMessage, responseSourceOverride);
    }

    private void expectReceiverAttach(final AttachMatcher attachMatcher, final AttachFrame attachResponse, final boolean refuseLink, boolean omitDetach,
                                     boolean deferAttachResponseWrite, Symbol errorType, String errorMessage, final Source responseSourceOverride)
    {
        // The response frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
        final FrameSender attachResponseSender = new FrameSender(this, FrameType.AMQP, -1, attachResponse, null);
        attachResponseSender.setValueProvider(new ValueProvider()
        {
            @Override
            public void setValues()
            {
                Object receivedHandle = attachMatcher.getReceivedHandle();

                attachResponseSender.setChannel(attachMatcher.getActualChannel());
                attachResponse.setHandle(receivedHandle);
                attachResponse.setName(attachMatcher.getReceivedName());
                attachResponse.setTarget(attachMatcher.getReceivedTarget());
                if(refuseLink) {
                    attachResponse.setSource(null);
                } else if(responseSourceOverride != null){
                    attachResponse.setSource(responseSourceOverride);
                } else {
                    attachResponse.setSource(createSourceObjectFromDescribedType(attachMatcher.getReceivedSource()));
                }

                _lastInitiatedLinkHandle = (UnsignedInteger) receivedHandle;
            }
        });

        if(deferAttachResponseWrite)
        {
            // Defer writing the attach frame until the subsequent frame is also ready
            attachResponseSender.setDeferWrite(true);
        }

        CompositeAmqpPeerRunnable composite = new CompositeAmqpPeerRunnable();
        composite.add(attachResponseSender);

        if (refuseLink && !omitDetach)
        {
            final DetachFrame detachResponse = new DetachFrame().setClosed(true);
            if (errorType != null)
            {
                org.apache.qpid.jms.test.testpeer.describedtypes.Error detachError = new org.apache.qpid.jms.test.testpeer.describedtypes.Error();

                detachError.setCondition(errorType);
                detachError.setDescription(errorMessage);

                detachResponse.setError(detachError);
            }

            // The response frame channel will be dynamically set based on the
            // incoming frame. Using the -1 is an illegal placeholder.
            final FrameSender detachResponseSender = new FrameSender(this, FrameType.AMQP, -1, detachResponse, null);
            detachResponseSender.setValueProvider(new ValueProvider()
            {
                @Override
                public void setValues()
                {
                    detachResponseSender.setChannel(attachMatcher.getActualChannel());
                    detachResponse.setHandle(attachMatcher.getReceivedHandle());
                }
            });

            composite.add(detachResponseSender);
        }

        attachMatcher.onCompletion(composite);

        addHandler(attachMatcher);
    }

    public void expectSharedDurableSubscriberAttach(String topicName, String subscriptionName, Matcher<?> linkNameMatcher, boolean clientIdSet) {
        expectSharedSubscriberAttach(topicName, subscriptionName, linkNameMatcher, true, false, clientIdSet, false, false);
    }

    public void expectSharedVolatileSubscriberAttach(String topicName, String subscriptionName, Matcher<?> linkNameMatcher, boolean clientIdSet) {
        expectSharedSubscriberAttach(topicName, subscriptionName, linkNameMatcher, false, false, clientIdSet, false, false);
    }

    public void expectSharedDurableSubscriberAttach(String topicName, String subscriptionName, Matcher<?> linkNameMatcher, boolean refuseLink, boolean clientIdSet) {
        expectSharedSubscriberAttach(topicName, subscriptionName, linkNameMatcher, true, refuseLink, clientIdSet, false, false);
    }

    public void expectSharedSubscriberAttach(String topicName, String subscriptionName, Matcher<?> linkNameMatcher, boolean durable, boolean refuseLink,
                                              boolean clientIdSet, boolean expectLinkCapability, boolean responseOffersLinkCapability)
    {
        Symbol[] sourceCapabilities;
        if(clientIdSet) {
            sourceCapabilities = new Symbol[] { AmqpDestinationHelper.TOPIC_CAPABILITY, AmqpSupport.SHARED };
        } else {
            sourceCapabilities = new Symbol[] { AmqpDestinationHelper.TOPIC_CAPABILITY, AmqpSupport.SHARED, AmqpSupport.GLOBAL };
        }

        SourceMatcher sourceMatcher = new SourceMatcher();
        sourceMatcher.withAddress(equalTo(topicName));
        sourceMatcher.withDynamic(equalTo(false));
        if(durable) {
            //TODO: will possibly be changed to a 1/config durability
            sourceMatcher.withDurable(equalTo(TerminusDurability.UNSETTLED_STATE));
            sourceMatcher.withExpiryPolicy(equalTo(TerminusExpiryPolicy.NEVER));
        } else {
            sourceMatcher.withDurable(equalTo(TerminusDurability.NONE));
            sourceMatcher.withExpiryPolicy(equalTo(TerminusExpiryPolicy.LINK_DETACH));
        }

        sourceMatcher.withCapabilities(arrayContaining(sourceCapabilities));

        // If we don't have the connection capability set we expect a desired link capability
        Matcher<?> linkDesiredCapabilitiesMatcher;
        if(expectLinkCapability) {
            linkDesiredCapabilitiesMatcher = arrayContaining(new Symbol[] { AmqpSupport.SHARED_SUBS });
        } else {
            linkDesiredCapabilitiesMatcher = nullValue();
        }

        // Generate offered capability response if supported
        Symbol[] linkOfferedCapabilitiesResponse = null;
        if(responseOffersLinkCapability) {
             linkOfferedCapabilitiesResponse = new Symbol[] { AmqpSupport.SHARED_SUBS };
        }

        expectReceiverAttach(linkNameMatcher, sourceMatcher, false, refuseLink, false, false, null, null, null, linkDesiredCapabilitiesMatcher, linkOfferedCapabilitiesResponse);
    }

    public void expectDurableSubscriberAttach(String topicName, String subscriptionName)
    {
        SourceMatcher sourceMatcher = new SourceMatcher();
        sourceMatcher.withAddress(equalTo(topicName));
        sourceMatcher.withDynamic(equalTo(false));
        //TODO: will possibly be changed to a 1/config durability
        sourceMatcher.withDurable(equalTo(TerminusDurability.UNSETTLED_STATE));
        sourceMatcher.withExpiryPolicy(equalTo(TerminusExpiryPolicy.NEVER));

        sourceMatcher.withCapabilities(arrayContaining(AmqpDestinationHelper.TOPIC_CAPABILITY));

        expectReceiverAttach(equalTo(subscriptionName), sourceMatcher);
    }

    public void expectDurableSubUnsubscribeNullSourceLookup(boolean failLookup, boolean shared, String subscriptionName, String topicName, boolean hasClientID) {
        String linkName = subscriptionName;
        if(!hasClientID) {
            linkName += AmqpSupport.SUB_NAME_DELIMITER + "global";
        }

        Matcher<String> linkNameMatcher = equalTo(linkName);
        Matcher<Object> nullSourceMatcher = nullValue();

        Source responseSourceOverride = null;
        Symbol errorType = null;
        String errorMessage = null;

        if(failLookup){
            errorType = AmqpError.NOT_FOUND;
            errorMessage = "No subscription link found";
        } else {
            responseSourceOverride = new Source();
            responseSourceOverride.setAddress(topicName);
            responseSourceOverride.setDynamic(false);
            //TODO: will possibly be changed to a 1/config durability
            responseSourceOverride.setDurable(TerminusDurability.UNSETTLED_STATE);
            responseSourceOverride.setExpiryPolicy(TerminusExpiryPolicy.NEVER);

            if(shared) {
                if(hasClientID) {
                    responseSourceOverride.setCapabilities(new Symbol[]{SHARED});
                } else {
                    responseSourceOverride.setCapabilities(new Symbol[]{SHARED, GLOBAL});
                }
            }
        }

        // If we don't have a ClientID, expect link capabilities to hint that we are trying
        // to reattach to a 'global' shared subscription.
        Matcher<?> linkDesiredCapabilitiesMatcher = null;
        if(!hasClientID) {
            linkDesiredCapabilitiesMatcher = arrayContaining(new Symbol[] { SHARED, GLOBAL });
        }

        expectReceiverAttach(linkNameMatcher, nullSourceMatcher, false, failLookup, false, false, errorType, errorMessage, responseSourceOverride, linkDesiredCapabilitiesMatcher, null);
    }

    public void expectDetach(boolean expectClosed, boolean sendResponse, boolean replyClosed)
    {
        expectDetach(expectClosed, sendResponse, replyClosed, null, null);
    }

    public void expectDetach(boolean expectClosed, boolean sendResponse, boolean replyClosed, Symbol errorType, String errorMessage)
    {
        Matcher<Boolean> closeMatcher = null;
        if(expectClosed)
        {
            closeMatcher = equalTo(true);
        }
        else
        {
            closeMatcher = Matchers.anyOf(equalTo(false), nullValue());
        }

        final DetachMatcher detachMatcher = new DetachMatcher().withClosed(closeMatcher);

        if (sendResponse)
        {
            final DetachFrame detachResponse = new DetachFrame();
            if(replyClosed)
            {
                detachResponse.setClosed(replyClosed);
            }

            if (errorType != null) {
                org.apache.qpid.jms.test.testpeer.describedtypes.Error detachError = new org.apache.qpid.jms.test.testpeer.describedtypes.Error();
                detachError.setCondition(errorType);
                detachError.setDescription(errorMessage);
                detachResponse.setError(detachError);
            } else {
                detachResponse.setError(null);
            }

            // The response frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
            final FrameSender detachResponseSender = new FrameSender(this, FrameType.AMQP, -1, detachResponse, null);
            detachResponseSender.setValueProvider(new ValueProvider()
            {
                @Override
                public void setValues()
                {
                    detachResponseSender.setChannel(detachMatcher.getActualChannel());
                    detachResponse.setHandle(detachMatcher.getReceivedHandle());
                }
            });

            detachMatcher.onCompletion(detachResponseSender);
        }

        addHandler(detachMatcher);
    }

    public void expectSessionFlow()
    {
        final FlowMatcher flowMatcher = new FlowMatcher()
                        .withLinkCredit(Matchers.nullValue())
                        .withHandle(Matchers.nullValue());

        addHandler(flowMatcher);
    }

    public void expectLinkFlow()
    {
        expectLinkFlow(false, false, Matchers.greaterThan(UnsignedInteger.ZERO));
    }

    public void expectLinkFlow(boolean drain, Matcher<UnsignedInteger> creditMatcher)
    {
        expectLinkFlow(drain, false, creditMatcher);
    }

    public void expectLinkFlow(boolean drain, boolean sendDrainFlowResponse, Matcher<UnsignedInteger> creditMatcher)
    {
        expectLinkFlowRespondWithTransfer(null, null, null, null, null, 0, drain, sendDrainFlowResponse, creditMatcher, null, false, false);
    }

    public void expectLinkFlowRespondWithTransfer(final HeaderDescribedType headerDescribedType,
                                                  final MessageAnnotationsDescribedType messageAnnotationsDescribedType,
                                                  final PropertiesDescribedType propertiesDescribedType,
                                                  final ApplicationPropertiesDescribedType appPropertiesDescribedType,
                                                  final DescribedType content)
    {
        expectLinkFlowRespondWithTransfer(headerDescribedType, messageAnnotationsDescribedType, propertiesDescribedType,
                                          appPropertiesDescribedType, content, 1);
    }

    public void expectLinkFlowRespondWithTransfer(final HeaderDescribedType headerDescribedType,
                                                  final MessageAnnotationsDescribedType messageAnnotationsDescribedType,
                                                  final PropertiesDescribedType propertiesDescribedType,
                                                  final ApplicationPropertiesDescribedType appPropertiesDescribedType,
                                                  final DescribedType content,
                                                  final boolean sendSettled)
    {
        expectLinkFlowRespondWithTransfer(headerDescribedType, messageAnnotationsDescribedType, propertiesDescribedType,
                                          appPropertiesDescribedType, content, 1, false, false,
                                          Matchers.greaterThanOrEqualTo(UnsignedInteger.valueOf(1)), 1, sendSettled, false);
    }

    public void expectLinkFlowRespondWithTransfer(final HeaderDescribedType headerDescribedType,
                                                  final MessageAnnotationsDescribedType messageAnnotationsDescribedType,
                                                  final PropertiesDescribedType propertiesDescribedType,
                                                  final ApplicationPropertiesDescribedType appPropertiesDescribedType,
                                                  final DescribedType content,
                                                  final int count)
    {
        expectLinkFlowRespondWithTransfer(headerDescribedType, messageAnnotationsDescribedType, propertiesDescribedType,
                                          appPropertiesDescribedType, content, count, false, false,
                                          Matchers.greaterThanOrEqualTo(UnsignedInteger.valueOf(count)), 1, false, false);
    }

    public void expectLinkFlowRespondWithTransfer(final HeaderDescribedType headerDescribedType,
                                                  final MessageAnnotationsDescribedType messageAnnotationsDescribedType,
                                                  final PropertiesDescribedType propertiesDescribedType,
                                                  ApplicationPropertiesDescribedType appPropertiesDescribedType,
                                                  final DescribedType content,
                                                  final int count,
                                                  final boolean drain,
                                                  final boolean sendDrainFlowResponse,
                                                  Matcher<UnsignedInteger> creditMatcher,
                                                  final Integer nextIncomingId,
                                                  boolean addMessageNumberProperty)
    {
        expectLinkFlowRespondWithTransfer(headerDescribedType, messageAnnotationsDescribedType, propertiesDescribedType,
                                          appPropertiesDescribedType, content, count, drain, sendDrainFlowResponse,
                                          creditMatcher, nextIncomingId, false, addMessageNumberProperty);
    }

    public void expectLinkFlowRespondWithTransfer(final HeaderDescribedType headerDescribedType,
            final MessageAnnotationsDescribedType messageAnnotationsDescribedType,
            final PropertiesDescribedType propertiesDescribedType,
            ApplicationPropertiesDescribedType appPropertiesDescribedType,
            final DescribedType content,
            final int count,
            final boolean drain,
            final boolean sendDrainFlowResponse,
            Matcher<UnsignedInteger> creditMatcher,
            final Integer nextIncomingId,
            final boolean sendSettled,
            boolean addMessageNumberProperty)
    {
        expectLinkFlowAndSendBackMessages(headerDescribedType, messageAnnotationsDescribedType, propertiesDescribedType,
                                          appPropertiesDescribedType, content, count, drain, sendDrainFlowResponse,
                                          creditMatcher, nextIncomingId, sendSettled, addMessageNumberProperty, 0, false);
    }

    public void expectLinkFlowAndSendBackMessages(final HeaderDescribedType headerDescribedType,
            final MessageAnnotationsDescribedType messageAnnotationsDescribedType,
            final PropertiesDescribedType propertiesDescribedType,
            ApplicationPropertiesDescribedType appPropertiesDescribedType,
            final DescribedType content,
            final int count,
            final boolean drain,
            final boolean sendDrainFlowResponse,
            Matcher<UnsignedInteger> creditMatcher,
            final Integer nextIncomingId,
            final boolean sendSettled,
            boolean addMessageNumberProperty,
            int msgPayloadPerFrame,
            boolean sendFinalTransferFrameWithoutPayload)
    {
        expectLinkFlowAndSendBackMessages(headerDescribedType, messageAnnotationsDescribedType, propertiesDescribedType,
                appPropertiesDescribedType, content, count, drain, sendDrainFlowResponse,
                creditMatcher, nextIncomingId, sendSettled, addMessageNumberProperty, msgPayloadPerFrame,
                sendFinalTransferFrameWithoutPayload, 0);
    }

    public void expectLinkFlowRespondWithTransfer(HeaderDescribedType headerDescribedType,
                                                  MessageAnnotationsDescribedType messageAnnotationsDescribedType,
                                                  PropertiesDescribedType propertiesDescribedType,
                                                  ApplicationPropertiesDescribedType appPropertiesDescribedType,
                                                  DescribedType content, boolean sendSettled, int messageFormat)
    {
        expectLinkFlowAndSendBackMessages(headerDescribedType, messageAnnotationsDescribedType, propertiesDescribedType,
                                          appPropertiesDescribedType, content, 1, false, false,
                                          Matchers.greaterThanOrEqualTo(UnsignedInteger.valueOf(1)),
                                          1, sendSettled, false, 0, false, messageFormat);
    }

    public void expectLinkFlowAndSendBackMessages(final HeaderDescribedType headerDescribedType,
            final MessageAnnotationsDescribedType messageAnnotationsDescribedType,
            final PropertiesDescribedType propertiesDescribedType,
            ApplicationPropertiesDescribedType appPropertiesDescribedType,
            final DescribedType content,
            final int count,
            final boolean drain,
            final boolean sendDrainFlowResponse,
            Matcher<UnsignedInteger> creditMatcher,
            final Integer nextIncomingId,
            final boolean sendSettled,
            boolean addMessageNumberProperty,
            int msgPayloadPerFrame,
            boolean sendFinalTransferFrameWithoutPayload,
            int messageFormat)
    {
        if (nextIncomingId == null && count > 0)
        {
            throw new IllegalArgumentException("The remote NextIncomingId must be specified if transfers have been requested");
        }

        Matcher<Boolean> drainMatcher = null;
        if(drain)
        {
            drainMatcher = equalTo(true);
        }
        else
        {
            drainMatcher = Matchers.anyOf(equalTo(false), nullValue());
        }

        Matcher<UnsignedInteger> remoteNextIncomingIdMatcher = null;
        if(nextIncomingId != null)
        {
             remoteNextIncomingIdMatcher = Matchers.equalTo(UnsignedInteger.valueOf(nextIncomingId));
        }
        else
        {
            remoteNextIncomingIdMatcher = Matchers.greaterThanOrEqualTo(UnsignedInteger.ONE);
        }

        final FlowMatcher flowMatcher = new FlowMatcher()
                        .withLinkCredit(creditMatcher)
                        .withDrain(drainMatcher)
                        .withNextIncomingId(remoteNextIncomingIdMatcher);

        CompositeAmqpPeerRunnable composite = new CompositeAmqpPeerRunnable();
        boolean addComposite = false;

        if (appPropertiesDescribedType == null && addMessageNumberProperty) {
            appPropertiesDescribedType = new ApplicationPropertiesDescribedType();
        }

        for(int i = 0; i < count; i++)
        {
            final int nextId = nextIncomingId + i;

            String tagString = "theDeliveryTag" + nextId;
            Binary dtag = new Binary(tagString.getBytes());

            if(addMessageNumberProperty) {
                appPropertiesDescribedType.setApplicationProperty(MESSAGE_NUMBER, i);
            }


            Binary payload = prepareTransferPayload(headerDescribedType, messageAnnotationsDescribedType,
                    propertiesDescribedType, appPropertiesDescribedType, content);

            int length = payload.getLength();
            int sent = 0;

            while (sent < length) {
                final TransferFrame transferFrame = new TransferFrame()
                        .setDeliveryId(UnsignedInteger.valueOf(nextId))
                        .setDeliveryTag(dtag)
                        .setMessageFormat(UnsignedInteger.valueOf(messageFormat))
                        .setSettled(sendSettled);

                int remaining = length - sent;
                Binary chunk;
                if(msgPayloadPerFrame != 0 && msgPayloadPerFrame < length) {
                    int chunkSize = Math.min(msgPayloadPerFrame, remaining);
                    chunk = payload.subBinary(sent, chunkSize);
                    sent += chunkSize;
                } else {
                    chunk = payload;
                    sent = length;
                }

                if(sent < length || (sent == length && sendFinalTransferFrameWithoutPayload)) {
                    // Indicate more frames if there is payload left, or we want to send a final transfer without payload
                    transferFrame.setMore(true);
                }

                // The response frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
                final FrameSender transferResponseSender = new FrameSender(this, FrameType.AMQP, -1, transferFrame, chunk);
                transferResponseSender.setValueProvider(new ValueProvider()
                {
                    @Override
                    public void setValues()
                    {
                        transferFrame.setHandle(flowMatcher.getReceivedHandle());
                        transferResponseSender.setChannel(flowMatcher.getActualChannel());
                    }
                });

                composite.add(transferResponseSender);
            }

            if(sendFinalTransferFrameWithoutPayload) {
                sendEmptyFinalTransfer(composite, flowMatcher, nextId, dtag, sendSettled);
            }

            addComposite = true;
        }

        if(drain && sendDrainFlowResponse)
        {
            final FlowFrame drainResponse = new FlowFrame();
            drainResponse.setOutgoingWindow(UnsignedInteger.ZERO); //TODO: shouldnt be hard coded
            drainResponse.setIncomingWindow(UnsignedInteger.valueOf(Integer.MAX_VALUE)); //TODO: shouldnt be hard coded
            drainResponse.setLinkCredit(UnsignedInteger.ZERO);
            drainResponse.setDrain(true);

            // The flow frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
            final FrameSender flowResponseSender = new FrameSender(this, FrameType.AMQP, -1, drainResponse, null);
            flowResponseSender.setValueProvider(new ValueProvider()
            {
                @Override
                public void setValues()
                {
                    flowResponseSender.setChannel(flowMatcher.getActualChannel());
                    drainResponse.setHandle(flowMatcher.getReceivedHandle());
                    drainResponse.setDeliveryCount(calculateNewDeliveryCount(flowMatcher));
                    drainResponse.setNextOutgoingId(calculateNewOutgoingId(flowMatcher, count));
                    drainResponse.setNextIncomingId(flowMatcher.getReceivedNextOutgoingId());
                }
            });

            addComposite = true;
            composite.add(flowResponseSender);
        }

        if(addComposite) {
            flowMatcher.onCompletion(composite);
        }

        addHandler(flowMatcher);
    }

    private void sendEmptyFinalTransfer(CompositeAmqpPeerRunnable composite, final FlowMatcher flowMatcher, final int deliveryId, Binary dTag, final boolean settled) {
        final TransferFrame transferFrame = new TransferFrame()
                .setDeliveryId(UnsignedInteger.valueOf(deliveryId))
                .setDeliveryTag(dTag)
                .setMessageFormat(UnsignedInteger.ZERO)
                .setSettled(settled);

        // The response frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
        final FrameSender finalEmptyTransferSender = new FrameSender(this, FrameType.AMQP, -1, transferFrame, null);
        finalEmptyTransferSender.setValueProvider(new ValueProvider()
        {
            @Override
            public void setValues()
            {
                transferFrame.setHandle(flowMatcher.getReceivedHandle());
                finalEmptyTransferSender.setChannel(flowMatcher.getActualChannel());
            }
        });

        composite.add(finalEmptyTransferSender);
    }

    public void expectLinkFlowThenPerformUnexpectedDeliveryCountAdvanceThenCreditTopupThenTransfers(final int prefetch, final int topUp, final int messageCount)
    {
        final FlowMatcher flowMatcher = new FlowMatcher()
                        .withHandle(notNullValue())
                        .withLinkCredit(equalTo(UnsignedInteger.valueOf(prefetch)))
                        .withDrain(Matchers.anyOf(equalTo(false), nullValue()));

        final FlowFrame advancingFlowResponse = new FlowFrame();
        advancingFlowResponse.setOutgoingWindow(UnsignedInteger.MAX_VALUE);
        advancingFlowResponse.setIncomingWindow(UnsignedInteger.valueOf(Integer.MAX_VALUE));

        // The flow frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
        final FrameSender advancingFlowResponseSender = new FrameSender(this, FrameType.AMQP, -1, advancingFlowResponse, null);
        advancingFlowResponseSender.setValueProvider(new ValueProvider()
        {
            @Override
            public void setValues()
            {
                advancingFlowResponseSender.setChannel(flowMatcher.getActualChannel());
                advancingFlowResponse.setHandle(flowMatcher.getReceivedHandle());
                advancingFlowResponse.setDeliveryCount(calculateNewDeliveryCount(flowMatcher, prefetch - topUp));
                advancingFlowResponse.setLinkCredit(UnsignedInteger.ZERO);
                advancingFlowResponse.setNextOutgoingId(flowMatcher.getReceivedNextIncomingId());
                advancingFlowResponse.setNextIncomingId(flowMatcher.getReceivedNextOutgoingId());
            }
        });

        final FlowFrame topUpFlowResponse = new FlowFrame();
        topUpFlowResponse.setOutgoingWindow(UnsignedInteger.MAX_VALUE);
        topUpFlowResponse.setIncomingWindow(UnsignedInteger.valueOf(Integer.MAX_VALUE));

        // The flow frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
        final FrameSender topUpFlowResponseSender = new FrameSender(this, FrameType.AMQP, -1, topUpFlowResponse, null);
        topUpFlowResponseSender.setValueProvider(new ValueProvider()
        {
            @Override
            public void setValues()
            {
                topUpFlowResponseSender.setChannel(flowMatcher.getActualChannel());
                topUpFlowResponse.setHandle(flowMatcher.getReceivedHandle());
                topUpFlowResponse.setDeliveryCount(calculateNewDeliveryCount(flowMatcher, prefetch - topUp));
                topUpFlowResponse.setLinkCredit(UnsignedInteger.valueOf(topUp));
                topUpFlowResponse.setNextOutgoingId(flowMatcher.getReceivedNextIncomingId());
                topUpFlowResponse.setNextIncomingId(flowMatcher.getReceivedNextOutgoingId());
            }
        });

        CompositeAmqpPeerRunnable composite = new CompositeAmqpPeerRunnable();
        composite.add(advancingFlowResponseSender);
        advancingFlowResponseSender.setDeferWrite(true);
        composite.add(topUpFlowResponseSender);

        final Integer remoteNextIncomingId = 0; //TODO: make configurable
        for(int i = 0; i < messageCount; i++)
        {
            final int nextId = remoteNextIncomingId + i;

            String tagString = "theDeliveryTag" + nextId;
            Binary dtag = new Binary(tagString.getBytes());

            final TransferFrame transferResponse = new TransferFrame()
            .setDeliveryId(UnsignedInteger.valueOf(nextId))
            .setDeliveryTag(dtag)
            .setMessageFormat(UnsignedInteger.ZERO)
            .setSettled(false);

            Binary payload = prepareTransferPayload(null, null, null, null, new AmqpValueDescribedType("content"));

            // The response frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
            final FrameSender transferResponseSender = new FrameSender(this, FrameType.AMQP, -1, transferResponse, payload);
            transferResponseSender.setValueProvider(new ValueProvider()
            {
                @Override
                public void setValues()
                {
                    transferResponse.setHandle(flowMatcher.getReceivedHandle());
                    transferResponseSender.setChannel(flowMatcher.getActualChannel());
                }
            });

            if(i != messageCount -1) {
                // Ensure all but the last transfer are set to defer, ensure they go in one write.
                transferResponseSender.setDeferWrite(true);
            }

            composite.add(transferResponseSender);
        }

        flowMatcher.onCompletion(composite);

        addHandler(flowMatcher);
    }

    private UnsignedInteger calculateNewDeliveryCount(FlowMatcher flowMatcher) {
        UnsignedInteger dc = (UnsignedInteger) flowMatcher.getReceivedDeliveryCount();
        UnsignedInteger lc = (UnsignedInteger) flowMatcher.getReceivedLinkCredit();

        return dc.add(lc);
    }

    private UnsignedInteger calculateNewDeliveryCount(FlowMatcher flowMatcher, int creditToConsume) {
        UnsignedInteger dc = (UnsignedInteger) flowMatcher.getReceivedDeliveryCount();
        UnsignedInteger lc = (UnsignedInteger) flowMatcher.getReceivedLinkCredit();

        assertThat(UnsignedInteger.valueOf(creditToConsume), lessThan(lc));

        return dc.add(UnsignedInteger.valueOf(creditToConsume));
    }

    private UnsignedInteger calculateNewOutgoingId(FlowMatcher flowMatcher, int sentCount) {
        UnsignedInteger nid = (UnsignedInteger) flowMatcher.getReceivedNextIncomingId();

        return nid.add(UnsignedInteger.valueOf(sentCount));
    }

    /**
     * Encodes and returns transfer payload Binary, or null if no message sections were supplied.
     */
    private Binary prepareTransferPayload(final HeaderDescribedType headerDescribedType,
                                          final MessageAnnotationsDescribedType messageAnnotationsDescribedType,
                                          final PropertiesDescribedType propertiesDescribedType,
                                          final ApplicationPropertiesDescribedType appPropertiesDescribedType,
                                          final DescribedType content)
    {
        Data payloadData = Data.Factory.create();
        boolean hasSection = false;

        if(headerDescribedType != null)
        {
            hasSection = true;
            payloadData.putDescribedType(headerDescribedType);
        }

        if(messageAnnotationsDescribedType != null)
        {
            hasSection = true;
            payloadData.putDescribedType(messageAnnotationsDescribedType);
        }

        if(propertiesDescribedType != null)
        {
            hasSection = true;
            payloadData.putDescribedType(propertiesDescribedType);
        }

        if(appPropertiesDescribedType != null)
        {
            hasSection = true;
            payloadData.putDescribedType(appPropertiesDescribedType);
        }

        if(content != null)
        {
            hasSection = true;
            payloadData.putDescribedType(content);
        }

        if (hasSection)
        {
            return payloadData.encode();
        }
        else
        {
            return null;
        }
    }

    public void expectTransferButDoNotRespond(Matcher<Binary> expectedPayloadMatcher)
    {
        expectTransfer(expectedPayloadMatcher, nullValue(), false, false, null, false);
    }

    public void expectTransferButDoNotRespond(Matcher<Binary> expectedPayloadMatcher, Matcher<Binary> deliveryTagMatcher)
    {
        expectTransfer(expectedPayloadMatcher, deliveryTagMatcher, nullValue(), false, false, null, false, 0, 0);
    }

    public void expectTransfer(Matcher<Binary> expectedPayloadMatcher)
    {
        expectTransfer(expectedPayloadMatcher, nullValue(), false, true, new Accepted(), true);
    }

    public void expectTransfer(Matcher<Binary> expectedPayloadMatcher, Matcher<Binary> deliveryTagMatcher)
    {
        expectTransfer(expectedPayloadMatcher, deliveryTagMatcher, nullValue(), false, true, new Accepted(), true, 0, 0);
    }

    public void expectTransfer(int frameSize)
    {
        expectTransfer(null, nullValue(), false, true, new Accepted(), true, frameSize, 0);
    }

    public void expectTransfer(Matcher<Binary> expectedPayloadMatcher, Matcher<?> stateMatcher,
                               ListDescribedType responseState, boolean responseSettled)
    {
        expectTransfer(expectedPayloadMatcher, stateMatcher, false, true, responseState, responseSettled);
    }

    public void expectTransfer(Matcher<Binary> expectedPayloadMatcher, Matcher<?> stateMatcher, boolean settled,
                               boolean sendResponseDisposition, ListDescribedType responseState, boolean responseSettled)
    {
        expectTransfer(expectedPayloadMatcher, stateMatcher, settled, sendResponseDisposition, responseState, responseSettled, 0, 0);
    }

    public void expectTransfer(Matcher<Binary> expectedPayloadMatcher,
            Matcher<?> stateMatcher, boolean settled, boolean sendResponseDisposition, ListDescribedType responseState,
            boolean responseSettled, int frameSize, long dispositionDelay)
    {
        expectTransfer(expectedPayloadMatcher, null, stateMatcher, settled, sendResponseDisposition, responseState, responseSettled, frameSize, dispositionDelay);
    }

    public void expectTransfer(Matcher<Binary> expectedPayloadMatcher, Matcher<Binary> deliveryTagMatcher,
            Matcher<?> stateMatcher, boolean settled, boolean sendResponseDisposition, ListDescribedType responseState,
            boolean responseSettled, int frameSize, long dispositionDelay)
    {
        expectTransfer(expectedPayloadMatcher, deliveryTagMatcher, stateMatcher, settled, sendResponseDisposition, responseState, responseSettled, frameSize, dispositionDelay, null);
    }

    public void expectTransfer(Matcher<Binary> expectedPayloadMatcher, int messageFormat)
    {
        expectTransfer(expectedPayloadMatcher, null, nullValue(), false, true, new Accepted(), true, 0, 0, Matchers.equalTo(UnsignedInteger.valueOf(messageFormat)));
    }

    //TODO: fix responseState to only admit applicable types.
    public void expectTransfer(Matcher<Binary> expectedPayloadMatcher, Matcher<Binary> deliveryTagMatcher,
            Matcher<?> stateMatcher, boolean settled, boolean sendResponseDisposition, ListDescribedType responseState,
            boolean responseSettled, int frameSize, long dispositionDelay, Matcher<UnsignedInteger> messageFormatMatcher)
    {
        Matcher<Boolean> settledMatcher = null;
        if(settled)
        {
            settledMatcher = equalTo(true);
        }
        else
        {
            settledMatcher = Matchers.anyOf(equalTo(false), nullValue());
        }

        final TransferMatcher transferMatcher = new TransferMatcher(frameSize);
        transferMatcher.setPayloadMatcher(expectedPayloadMatcher);
        transferMatcher.withSettled(settledMatcher);
        transferMatcher.withState(stateMatcher);
        if (messageFormatMatcher != null) {
            transferMatcher.withMessageFormat(messageFormatMatcher);
        }
        if (deliveryTagMatcher != null) {
            transferMatcher.withDeliveryTag(deliveryTagMatcher);
        }

        if(sendResponseDisposition) {
            final DispositionFrame dispositionResponse = new DispositionFrame()
                                                       .setRole(Role.RECEIVER)
                                                       .setSettled(responseSettled)
                                                       .setState(responseState);

            // The response frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
            final FrameSender dispositionFrameSender = new FrameSender(this, FrameType.AMQP, -1, dispositionResponse, null);
            dispositionFrameSender.setSendDelay(dispositionDelay);
            dispositionFrameSender.setValueProvider(new ValueProvider()
            {
                @Override
                public void setValues()
                {
                    dispositionFrameSender.setChannel(transferMatcher.getActualChannel());
                    dispositionResponse.setFirst(transferMatcher.getReceivedDeliveryId());
                }
            });

            transferMatcher.onCompletion(dispositionFrameSender);
        }

        addHandler(transferMatcher);
    }

    public void expectTransferRespondWithDrain(Matcher<Binary> expectedPayloadMatcher, int sentMessages)
    {
        expectTransferRespondWithDrain(expectedPayloadMatcher, DEFAULT_PRODUCER_CREDIT, sentMessages);
    }

    public void expectTransferRespondWithDrain(Matcher<Binary> expectedPayloadMatcher, int originalCredit, int sentMessages)
    {
        Matcher<Boolean> settledMatcher = Matchers.anyOf(equalTo(false), nullValue());

        final TransferMatcher transferMatcher = new TransferMatcher(0);
        transferMatcher.setPayloadMatcher(expectedPayloadMatcher);
        transferMatcher.withSettled(settledMatcher);
        transferMatcher.withState(nullValue());

        CompositeAmqpPeerRunnable composite = new CompositeAmqpPeerRunnable();
        final DispositionFrame dispositionResponse = new DispositionFrame().setRole(Role.RECEIVER).setSettled(true).setState(new Accepted());

        // The response frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
        final FrameSender dispositionFrameSender = new FrameSender(this, FrameType.AMQP, -1, dispositionResponse, null);
        dispositionFrameSender.setValueProvider(new ValueProvider()
        {
            @Override
            public void setValues()
            {
                dispositionFrameSender.setChannel(transferMatcher.getActualChannel());
                dispositionResponse.setFirst(transferMatcher.getReceivedDeliveryId());
            }
        });

        final FlowFrame flowFrame = new FlowFrame().setNextIncomingId(UnsignedInteger.ONE.add(UnsignedInteger.valueOf(sentMessages))) //TODO: start point shouldnt be hard coded
            .setIncomingWindow(UnsignedInteger.valueOf(2048))
            .setNextOutgoingId(UnsignedInteger.ONE) //TODO: shouldnt be hard coded
            .setOutgoingWindow(UnsignedInteger.valueOf(2048))
            .setDeliveryCount(UnsignedInteger.valueOf(sentMessages))
            .setLinkCredit(UnsignedInteger.valueOf(originalCredit - sentMessages))
            .setDrain(true);

        // The flow frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
        final FrameSender flowFrameSender = new FrameSender(this, FrameType.AMQP, -1, flowFrame, null);
        flowFrameSender.setValueProvider(new ValueProvider()
        {
            @Override
            public void setValues()
            {
                flowFrameSender.setChannel(transferMatcher.getActualChannel());
                flowFrame.setHandle(transferMatcher.getReceivedHandle());
            }
        });

        composite.add(flowFrameSender);
        composite.add(dispositionFrameSender);

        transferMatcher.onCompletion(composite);

        addHandler(transferMatcher);
    }

    public void expectDeclare(Binary txnId)
    {
        TransferPayloadCompositeMatcher declareMatcher = new TransferPayloadCompositeMatcher();
        declareMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(new Declare()));

        expectTransfer(declareMatcher, nullValue(), new Declared().setTxnId(txnId), true);
    }

    public void expectDeclareButDoNotRespond()
    {
        TransferPayloadCompositeMatcher declareMatcher = new TransferPayloadCompositeMatcher();
        declareMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(new Declare()));

        expectTransfer(declareMatcher, nullValue(), false, false, null, false);
    }

    public void expectDeclareAndReject()
    {
        TransferPayloadCompositeMatcher declareMatcher = new TransferPayloadCompositeMatcher();
        declareMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(new Declare()));

        expectTransfer(declareMatcher, nullValue(), new Rejected(), true);
    }

    public void expectDischarge(Binary txnId, boolean dischargeState) {
        expectDischarge(txnId, dischargeState, new Accepted());
    }

    public void expectDischarge(Binary txnId, boolean dischargeState, ListDescribedType responseState) {
        // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
        // and reply with given response and settled disposition to indicate the outcome.
        Discharge discharge = new Discharge();
        discharge.setFail(dischargeState);
        discharge.setTxnId(txnId);

        TransferPayloadCompositeMatcher dischargeMatcher = new TransferPayloadCompositeMatcher();
        dischargeMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(discharge));

        expectTransfer(dischargeMatcher, nullValue(), responseState, true);
    }

    public void expectDischargeButDoNotRespond(Binary txnId, boolean dischargeState) {
        // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
        // and reply with given response and settled disposition to indicate the outcome.
        Discharge discharge = new Discharge();
        discharge.setFail(dischargeState);
        discharge.setTxnId(txnId);

        TransferPayloadCompositeMatcher dischargeMatcher = new TransferPayloadCompositeMatcher();
        dischargeMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(discharge));

        expectTransfer(dischargeMatcher, nullValue(), false, false, null, true);
    }

    public void remotelyCloseLastCoordinatorLink()
    {
        remotelyCloseLastCoordinatorLink(true, true, TransactionError.TRANSACTION_ROLLBACK, "Discharge of TX failed.");
    }

    public void remotelyCloseLastCoordinatorLink(Symbol errorType, String errorMessage)
    {
        remotelyCloseLastCoordinatorLink(true, true, errorType, errorMessage);
    }

    public void remotelyCloseLastCoordinatorLink(boolean expectDetachResponse, boolean closed, Symbol errorType, String errorMessage)
    {
        // Now remotely end the last attached transaction coordinator
        synchronized (_handlersLock) {
            CompositeAmqpPeerRunnable comp = insertCompsiteActionForLastHandler();

            // Now generate the Detach for the appropriate link on the appropriate session
            final DetachFrame detachFrame = new DetachFrame();
            detachFrame.setClosed(true);
            if (errorType != null) {
                org.apache.qpid.jms.test.testpeer.describedtypes.Error detachError = new org.apache.qpid.jms.test.testpeer.describedtypes.Error();
                detachError.setCondition(errorType);
                detachError.setDescription(errorMessage);
                detachFrame.setError(detachError);
            }

            // The response frame channel will be dynamically set based on the previous frames. Using the -1 is an illegal placeholder.
            final FrameSender frameSender = new FrameSender(this, FrameType.AMQP, -1, detachFrame, null);
            frameSender.setValueProvider(new ValueProvider() {
                @Override
                public void setValues() {
                    frameSender.setChannel(_lastInitiatedChannel);
                    detachFrame.setHandle(_lastInitiatedCoordinatorLinkHandle);
                }
            });
            comp.add(frameSender);

            if (expectDetachResponse) {
                Matcher<Boolean> closeMatcher = null;
                if (closed) {
                    closeMatcher = equalTo(true);
                } else {
                    closeMatcher = Matchers.anyOf(equalTo(false), nullValue());
                }

                // Expect a response to our Detach.
                final DetachMatcher detachMatcher = new DetachMatcher().withClosed(closeMatcher);
                // TODO: enable matching on the channel number of the response.
                addHandler(detachMatcher);
            }
        }
    }

    public void remotelyCloseLastCoordinatorLinkOnDischarge(Binary txnId, boolean dischargeState)
    {
        remotelyCloseLastCoordinatorLinkOnDischarge(txnId, dischargeState, true, true, TransactionError.TRANSACTION_ROLLBACK, "Discharge of TX failed.", false, null);
    }

    public void remotelyCloseLastCoordinatorLinkOnDischarge(Binary txnId, boolean dischargeState, boolean pipelinedDeclare, Binary nextTxnId)
    {
        remotelyCloseLastCoordinatorLinkOnDischarge(txnId, dischargeState, true, true, TransactionError.TRANSACTION_ROLLBACK, "Discharge of TX failed.", pipelinedDeclare, nextTxnId);
    }

    public void remotelyCloseLastCoordinatorLinkOnDischarge(Binary txnId, boolean dischargeState, Symbol errorType, String errorMessage)
    {
        remotelyCloseLastCoordinatorLinkOnDischarge(txnId, dischargeState, true, true, errorType, errorMessage, false, null);
    }

    public void remotelyCloseLastCoordinatorLinkOnDischarge(Binary txnId, boolean dischargeState, boolean expectDetachResponse, boolean closed, Symbol errorType, String errorMessage, boolean pipelinedDeclare, Binary nextTxnId) {
        // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
        // and reply with given response and settled disposition to indicate the outcome.
        Discharge discharge = new Discharge();
        discharge.setFail(dischargeState);
        discharge.setTxnId(txnId);

        TransferPayloadCompositeMatcher dischargeMatcher = new TransferPayloadCompositeMatcher();
        dischargeMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(discharge));

        expectTransfer(dischargeMatcher, nullValue(), false, false, null, false);

        if (pipelinedDeclare) {
            Declare declare = new Declare();

            TransferPayloadCompositeMatcher declareMatcher = new TransferPayloadCompositeMatcher();
            declareMatcher.setMessageContentMatcher(new EncodedAmqpValueMatcher(declare));

            expectTransfer(declareMatcher, nullValue(), false, false, new Declared().setTxnId(nextTxnId), true);
        }

        remotelyCloseLastCoordinatorLink(expectDetachResponse, closed, errorType, errorMessage);
    }

    public void expectDispositionThatIsAcceptedAndSettled()
    {
        expectDisposition(true, new DescriptorMatcher(Accepted.DESCRIPTOR_CODE, Accepted.DESCRIPTOR_SYMBOL));
    }

    public void expectDispositionThatIsReleasedAndSettled()
    {
        expectDisposition(true, new DescriptorMatcher(Released.DESCRIPTOR_CODE, Released.DESCRIPTOR_SYMBOL));
    }

    public void expectDisposition(boolean settled, Matcher<?> stateMatcher)
    {
        expectDisposition(settled, stateMatcher, null, null);
    }

    public void expectDisposition(boolean settled, Matcher<?> stateMatcher, Integer firstDeliveryId, Integer lastDeliveryId)
    {
        Matcher<Boolean> settledMatcher = null;
        if(settled)
        {
            settledMatcher = equalTo(true);
        }
        else
        {
            settledMatcher = Matchers.anyOf(equalTo(false), nullValue());
        }

        Matcher<?> firstDeliveryIdMatcher = notNullValue();
        if(firstDeliveryId != null) {
            firstDeliveryIdMatcher = equalTo(UnsignedInteger.valueOf(firstDeliveryId));
        }

        Matcher<?> lastDeliveryIdMatcher = notNullValue();
        if(lastDeliveryId != null) {
            lastDeliveryIdMatcher = equalTo(UnsignedInteger.valueOf(lastDeliveryId));
        }

        addHandler(new DispositionMatcher()
            .withSettled(settledMatcher)
            .withFirst(firstDeliveryIdMatcher)
            .withLast(lastDeliveryIdMatcher)
            .withState(stateMatcher));
    }

    private Object createTargetObjectFromDescribedType(Object o) {
        assertThat(o, instanceOf(DescribedType.class));
        Object described = ((DescribedType) o).getDescribed();
        assertThat(described, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> targetFields = (List<Object>) described;
        Object descriptor = ((DescribedType) o).getDescriptor();
        if (descriptor == Target.DESCRIPTOR_CODE || descriptor.equals(Target.DESCRIPTOR_SYMBOL)) {
            return new Target(targetFields.toArray());
        } else if (descriptor == Coordinator.DESCRIPTOR_CODE || descriptor.equals(Coordinator.DESCRIPTOR_SYMBOL)) {
            return new Coordinator(targetFields.toArray());
        } else {
            throw new IllegalArgumentException("Unexpected target descriptor: " + descriptor);
        }
    }

    private Source createSourceObjectFromDescribedType(Object o) {
        assertThat(o, instanceOf(DescribedType.class));
        Object described = ((DescribedType) o).getDescribed();
        assertThat(described, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> sourceFields = (List<Object>) described;

        return new Source(sourceFields.toArray());
    }

    public void remotelyEndLastOpenedSession(boolean expectEndResponse) {
        remotelyEndLastOpenedSession(expectEndResponse, 0, null, null);
    }

    public void remotelyEndLastOpenedSession(boolean expectEndResponse, long delayBeforeSend) {
        remotelyEndLastOpenedSession(expectEndResponse, delayBeforeSend, null, null);
    }

    public void remotelyEndLastOpenedSession(boolean expectEndResponse, final long delayBeforeSend, Symbol errorType, String errorMessage) {
        synchronized (_handlersLock) {
            CompositeAmqpPeerRunnable comp = insertCompsiteActionForLastHandler();

            // Now generate the End for the appropriate session
            final EndFrame endFrame = new EndFrame();
            if (errorType != null) {
                org.apache.qpid.jms.test.testpeer.describedtypes.Error detachError = new org.apache.qpid.jms.test.testpeer.describedtypes.Error();
                detachError.setCondition(errorType);
                detachError.setDescription(errorMessage);
                endFrame.setError(detachError);
            }

            int channel = -1;
            final FrameSender frameSender = new FrameSender(this, FrameType.AMQP, channel, endFrame, null);
            frameSender.setValueProvider(new ValueProvider() {
                @Override
                public void setValues() {
                    frameSender.setChannel(_lastInitiatedChannel);

                    //Insert a delay if requested
                    if (delayBeforeSend > 0) {
                        try {
                            Thread.sleep(delayBeforeSend);
                        } catch (InterruptedException e) {
                            // Ignore
                        }
                    }
                }
            });
            comp.add(frameSender);

            if (expectEndResponse) {
                // Expect a response to our End.
                final EndMatcher endMatcher = new EndMatcher();
                // TODO: enable matching on the channel number of the response.
                addHandler(endMatcher);
            }
        }
    }

    public void remotelyCloseConnection(boolean expectCloseResponse) {
        remotelyCloseConnection(expectCloseResponse, null, null, null, 0);
    }

    public void remotelyCloseConnection(boolean expectCloseResponse, Symbol errorType, String errorMessage) {
        remotelyCloseConnection(expectCloseResponse, errorType, errorMessage, null, 0);
    }

    public void remotelyCloseConnection(boolean expectCloseResponse, Symbol errorType, String errorMessage, long delayBeforeSend) {
        remotelyCloseConnection(expectCloseResponse, errorType, errorMessage, null, delayBeforeSend);
    }

    public void remotelyCloseConnection(boolean expectCloseResponse, Symbol errorType, String errorMessage, Map<Symbol, Object> info) {
        remotelyCloseConnection(expectCloseResponse, errorType, errorMessage, info, 0);
    }

    public void remotelyCloseConnection(boolean expectCloseResponse, Symbol errorType, String errorMessage, Map<Symbol, Object> info, long delayBeforeSend) {
        synchronized (_handlersLock) {
            // Prepare a composite to insert this action at the end of the handler sequence
            CompositeAmqpPeerRunnable comp = insertCompsiteActionForLastHandler();

            // Now generate the Close
            final FrameSender closeSender = createCloseFrameSender(errorType, errorMessage, info, delayBeforeSend);

            comp.add(closeSender);

            if (expectCloseResponse) {
                // Expect a response to our Close.
                final CloseMatcher closeMatcher = new CloseMatcher();
                addHandler(closeMatcher);
            }
        }
    }

    private FrameSender createCloseFrameSender(Symbol errorType, String errorMessage, Map<Symbol, Object> info, final long delayBeforeSend) {
        final CloseFrame closeFrame = new CloseFrame();
        if (errorType != null) {
            org.apache.qpid.jms.test.testpeer.describedtypes.Error closeError = new org.apache.qpid.jms.test.testpeer.describedtypes.Error();
            closeError.setCondition(errorType);
            closeError.setDescription(errorMessage);
            closeError.setInfo(info);

            closeFrame.setError(closeError);
        }

        final FrameSender closeSender = new FrameSender(this, FrameType.AMQP, CONNECTION_CHANNEL, closeFrame, null);
        closeSender.setValueProvider(new ValueProvider() {

            @Override
            public void setValues() {
                //Insert a delay if requested
                if (delayBeforeSend > 0) {
                    try {
                        Thread.sleep(delayBeforeSend);
                    } catch (InterruptedException e) {
                        // Ignore
                    }
                }
            }
        });

        return closeSender;
    }

    void sendConnectionCloseImmediately(Symbol errorType, String errorMessage) {
        FrameSender closeSender = createCloseFrameSender(errorType, errorMessage, null, 0);
        closeSender.run();
    }

    public void remotelyDetachLastOpenedLinkOnLastOpenedSession(boolean expectDetachResponse, boolean closed) {
        remotelyDetachLastOpenedLinkOnLastOpenedSession(expectDetachResponse, closed, null, null);
    }

    public void remotelyDetachLastOpenedLinkOnLastOpenedSession(boolean expectDetachResponse, boolean closed, Symbol errorType, String errorMessage) {
        remotelyDetachLastOpenedLinkOnLastOpenedSession(expectDetachResponse, closed, errorType, errorMessage, 0);
    }

    public void remotelyDetachLastOpenedLinkOnLastOpenedSession(boolean expectDetachResponse, boolean closed, Symbol errorType, String errorMessage, final long delayBeforeSend) {
        synchronized (_handlersLock) {
            CompositeAmqpPeerRunnable comp = insertCompsiteActionForLastHandler();

            // Now generate the Detach for the appropriate link on the appropriate session
            final DetachFrame detachFrame = new DetachFrame();
            detachFrame.setClosed(closed);
            if (errorType != null) {
                org.apache.qpid.jms.test.testpeer.describedtypes.Error detachError = new org.apache.qpid.jms.test.testpeer.describedtypes.Error();
                detachError.setCondition(errorType);
                detachError.setDescription(errorMessage);
                detachFrame.setError(detachError);
            }

            // The response frame channel will be dynamically set based on the previous frames. Using the -1 is an illegal placeholder.
            final FrameSender frameSender = new FrameSender(this, FrameType.AMQP, -1, detachFrame, null);
            frameSender.setValueProvider(new ValueProvider() {
                @Override
                public void setValues() {
                    frameSender.setChannel(_lastInitiatedChannel);
                    detachFrame.setHandle(_lastInitiatedLinkHandle);

                    //Insert a delay if requested
                    if (delayBeforeSend > 0) {
                        try {
                            Thread.sleep(delayBeforeSend);
                        } catch (InterruptedException e) {
                            // Ignore
                        }
                    }
                }
            });
            comp.add(frameSender);

            if (expectDetachResponse) {
                Matcher<Boolean> closeMatcher = null;
                if (closed) {
                    closeMatcher = equalTo(true);
                } else {
                    closeMatcher = Matchers.anyOf(equalTo(false), nullValue());
                }

                // Expect a response to our Detach.
                final DetachMatcher detachMatcher = new DetachMatcher().withClosed(closeMatcher);
                // TODO: enable matching on the channel number of the response.
                addHandler(detachMatcher);
            }
        }
    }

    private CompositeAmqpPeerRunnable insertCompsiteActionForLastHandler() {
        CompositeAmqpPeerRunnable comp = new CompositeAmqpPeerRunnable();
        Handler h = getLastHandler();
        AmqpPeerRunnable orig = h.getOnCompletionAction();
        if (orig != null) {
            comp.add(orig);
        }
        h.onCompletion(comp);
        return comp;
    }

    public void sendTransferToLastOpenedLinkOnLastOpenedSession(final HeaderDescribedType headerDescribedType,
                                                                final MessageAnnotationsDescribedType messageAnnotationsDescribedType,
                                                                final PropertiesDescribedType propertiesDescribedType,
                                                                final ApplicationPropertiesDescribedType appPropertiesDescribedType,
                                                                final DescribedType content,
                                                                final int nextIncomingDeliveryId) {

        sendTransferToLastOpenedLinkOnLastOpenedSession(headerDescribedType, messageAnnotationsDescribedType, propertiesDescribedType, appPropertiesDescribedType, content, nextIncomingDeliveryId, null, null, 0);
    }

    public void sendTransferToLastOpenedLinkOnLastOpenedSession(final HeaderDescribedType headerDescribedType,
                                                                final MessageAnnotationsDescribedType messageAnnotationsDescribedType,
                                                                final PropertiesDescribedType propertiesDescribedType,
                                                                final ApplicationPropertiesDescribedType appPropertiesDescribedType,
                                                                final DescribedType content,
                                                                final int nextIncomingDeliveryId,
                                                                final String tagAsString,
                                                                final Boolean more,
                                                                final int sendDelay) {
        synchronized (_handlersLock) {
            CompositeAmqpPeerRunnable comp = insertCompsiteActionForLastHandler();

            String tagString = tagAsString;
            if(tagString == null) {
                tagString = "theDeliveryTag" + nextIncomingDeliveryId;
            }

            Binary dtag = new Binary(tagString.getBytes());

            final TransferFrame transferResponse = new TransferFrame()
            .setDeliveryId(UnsignedInteger.valueOf(nextIncomingDeliveryId))
            .setDeliveryTag(dtag)
            .setMessageFormat(UnsignedInteger.ZERO);
            if(more != null) {
                transferResponse.setMore(more);
            }

            Binary payload = prepareTransferPayload(headerDescribedType, messageAnnotationsDescribedType, propertiesDescribedType, appPropertiesDescribedType, content);

            // The response frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
            final FrameSender transferSender = new FrameSender(this, FrameType.AMQP, -1, transferResponse, payload);
            transferSender.setValueProvider(new ValueProvider()
            {
                @Override
                public void setValues()
                {
                    transferResponse.setHandle(_lastInitiatedLinkHandle);
                    transferSender.setChannel(_lastInitiatedChannel);
                }
            });

            if(sendDelay != 0) {
                transferSender.setSendDelay(sendDelay);
            }

            comp.add(transferSender);
        }
    }

    public void runAfterLastHandler(AmqpPeerRunnable action) {
        synchronized (_handlersLock) {
            // Prepare a composite to insert this action at the end of the handler sequence
            CompositeAmqpPeerRunnable comp = insertCompsiteActionForLastHandler();
            comp.add(action);
        }
    }

    void assertionFailed(AssertionError ae) {
        if(_firstAssertionError == null)
        {
            _firstAssertionError = ae;
        }
    }

    public void expectSaslHeaderThenDrop() {
        AmqpPeerRunnable exitAfterHeader = new AmqpPeerRunnable() {
            @Override
            public void run() {
                _driverRunnable.exitReadLoopEarly();
            }
        };

        addHandler(new HeaderHandlerImpl(AmqpHeader.SASL_HEADER, AmqpHeader.SASL_HEADER, exitAfterHeader));
    }


    public void dropAfterLastHandler() {
        dropAfterLastHandler(0);
    }

    public void dropAfterLastHandler(final long delay) {
        AmqpPeerRunnable exitEarly = new AmqpPeerRunnable() {
            @Override
            public void run() {
                if(delay > 0) {
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        LOGGER.warn("Interrupted while delaying before read loop exit");
                        Thread.currentThread().interrupt();
                    }
                }

                _driverRunnable.exitReadLoopEarly();
            }
        };

        runAfterLastHandler(exitEarly);
    }

    public void optionalFlow(final boolean drain, final boolean sendDrainFlowResponse,Matcher<UnsignedInteger> creditMatcher)
    {
        final FlowMatcher flowMatcher = new FlowMatcher();
        flowMatcher.setOptional(true);

        Matcher<Boolean> drainMatcher = null;
        if(drain)
        {
            drainMatcher = equalTo(true);
        }
        else
        {
            drainMatcher = Matchers.anyOf(equalTo(false), nullValue());
        }

        flowMatcher.withLinkCredit(creditMatcher);
        flowMatcher.withDrain(drainMatcher);

        if(drain && sendDrainFlowResponse)
        {
            final FlowFrame drainResponse = new FlowFrame();
            drainResponse.setOutgoingWindow(UnsignedInteger.ZERO); //TODO: shouldnt be hard coded
            drainResponse.setIncomingWindow(UnsignedInteger.valueOf(Integer.MAX_VALUE)); //TODO: shouldnt be hard coded
            drainResponse.setLinkCredit(UnsignedInteger.ZERO);
            drainResponse.setDrain(true);

            // The flow frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
            final FrameSender flowResponseSender = new FrameSender(this, FrameType.AMQP, -1, drainResponse, null);
            flowResponseSender.setValueProvider(new ValueProvider()
            {
                @Override
                public void setValues()
                {
                    flowResponseSender.setChannel(flowMatcher.getActualChannel());
                    drainResponse.setHandle(flowMatcher.getReceivedHandle());
                    drainResponse.setDeliveryCount(calculateNewDeliveryCount(flowMatcher));
                    drainResponse.setNextOutgoingId(calculateNewOutgoingId(flowMatcher, 0));
                    drainResponse.setNextIncomingId(flowMatcher.getReceivedNextOutgoingId());
                }
            });

            flowMatcher.onCompletion(flowResponseSender);
        }

        addHandler(flowMatcher);
    }
}
