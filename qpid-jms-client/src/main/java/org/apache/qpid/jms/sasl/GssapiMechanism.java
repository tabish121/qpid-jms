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

import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.apache.qpid.jms.util.PropertyUtil;
import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.engine.sasl.client.AbstractMechanism;
import org.apache.qpid.protonj2.engine.sasl.client.SaslCredentialsProvider;
import org.apache.qpid.protonj2.types.Symbol;

/**
 * Implements the GSSAPI sasl authentication Mechanism.
 */
public class GssapiMechanism extends AbstractMechanism {

    public static final Symbol NAME = Symbol.valueOf("GSSAPI");

    private Subject subject;
    private SaslClient saslClient;
    private String protocol = "amqp";
    private String serverName = null;
    private String configScope = "amqp-jms-client";

    // a gss/sasl service name, x@y, morphs to a krbPrincipal a/y@REALM

    @Override
    public Symbol getName() {
        return NAME;
    }

    @Override
    public boolean isEnabledByDefault() {
        // Only enable if given explicit configuration to do so, as we can't discern here
        // whether the external configuration is appropriately set to actually allow its use.
        return false;
    }

    @Override
    public boolean isApplicable(SaslCredentialsProvider credentials) {
        return true;
    }

    public void init(Map<String, Object> saslOptions) {
        if (saslOptions == null) {
            throw new IllegalArgumentException("Given Properties object cannot be null");
        }

        for (Map.Entry<String, Object> entry : saslOptions.entrySet()) {
            PropertyUtil.setProperty(this, entry.getKey(), entry.getValue());
        }
    }

    @Override
    public ProtonBuffer getInitialResponse(SaslCredentialsProvider credentials) throws SaslException {
        try {
            init(credentials.options());

            LoginContext loginContext = new LoginContext(configScope, new CredentialCallbackHandler(credentials));;

            loginContext.login();
            subject = loginContext.getSubject();

            return Subject.doAs(subject, new PrivilegedExceptionAction<ProtonBuffer>() {

                @Override
                public ProtonBuffer run() throws Exception {
                    Map<String, String> props = new HashMap<>();
                    props.put("javax.security.sasl.server.authentication", "true");

                    saslClient = Sasl.createSaslClient(new String[]{NAME.toString()}, null, protocol, serverName, props, null);
                    if (saslClient.hasInitialResponse()) {
                        return ProtonByteBufferAllocator.DEFAULT.wrap(saslClient.evaluateChallenge(new byte[0]));
                    }

                    return null;
                }
            });
        } catch (Exception e) {
            throw new SaslException(e.toString(), e);
        }
    }

    @Override
    public ProtonBuffer getChallengeResponse(SaslCredentialsProvider credentials, ProtonBuffer challenge) throws SaslException {
        try {
            return Subject.doAs(subject, new PrivilegedExceptionAction<ProtonBuffer>() {
                @Override
                public ProtonBuffer run() throws Exception {
                    byte[] input = new byte[challenge.getReadableBytes()];

                    // Copy the challenge into a byte array that the SaslClient can process.
                    challenge.readBytes(input);

                    return ProtonByteBufferAllocator.DEFAULT.wrap(saslClient.evaluateChallenge(input));
                }
            });
        } catch (PrivilegedActionException e) {
            throw new SaslException(e.toString(), e);
        }
    }

    @Override
    public void verifyCompletion() throws SaslException {
        boolean result = saslClient.isComplete();
        saslClient.dispose();
        if (!result) {
            throw new SaslException("not complete");
        }
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getServerName() {
        return serverName;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    public String getConfigScope() {
        return configScope;
    }

    public void setConfigScope(String configScope) {
        this.configScope = configScope;
    }

    private class CredentialCallbackHandler implements CallbackHandler {

        private final SaslCredentialsProvider credentials;

        CredentialCallbackHandler(SaslCredentialsProvider credentials) {
            this.credentials = credentials;
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (int i = 0; i < callbacks.length; i++) {
                Callback cb = callbacks[i];
                if (cb instanceof NameCallback) {
                    ((NameCallback) cb).setName(credentials.username());
                } else if (cb instanceof PasswordCallback) {
                    String pass = credentials.password();
                    if (pass != null) {
                        ((PasswordCallback) cb).setPassword(pass.toCharArray());
                    }
                } else {
                    throw new UnsupportedCallbackException(cb);
                }
            }
        }
    }
}
