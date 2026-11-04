/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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
package org.apache.flume.client.thrift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Properties;
import org.apache.flume.api.FlumeClientException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.RpcClientFactory;
import org.junit.Test;

/**
 * Checks that the {@code thrift} client type returns the Kerberos client when {@code kerberos} is enabled.
 * <p>
 * {@code ThriftSink} relies on this to support Kerberos.
 * The tests need no KDC: they check the errors that only the Kerberos client reports.
 * </p>
 */
public class TestThriftRpcClientProvider {

    private static Properties properties(boolean kerberos) {
        Properties props = new Properties();
        props.setProperty(
                RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE, RpcClientConfigurationConstants.THRIFT_CLIENT_TYPE);
        props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS, "h1");
        // The client connects lazily, so no server is needed.
        props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX + "h1", "localhost:4141");
        props.setProperty(RpcClientConfigurationConstants.KERBEROS_KEY, String.valueOf(kerberos));
        return props;
    }

    @Test
    public void returnsPlainClientWithoutKerberos() throws Exception {
        try (RpcClient client = RpcClientFactory.getInstance(properties(false))) {
            assertEquals(ThriftRpcClient.class, client.getClass());
        }
    }

    @Test
    public void returnsKerberosClientWithKerberos() {
        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> RpcClientFactory.getInstance(properties(true)));
        assertTrue(e.getMessage(), e.getMessage().contains("server principal"));
    }

    @Test
    public void kerberosClientRequiresLogin() {
        Properties props = properties(true);
        props.setProperty("server-principal", "flume/localhost@EXAMPLE.COM");
        FlumeClientException e = assertThrows(FlumeClientException.class, () -> RpcClientFactory.getInstance(props));
        assertTrue(e.getMessage(), e.getMessage().contains("Authentication failed in Kerberos mode"));
    }
}
