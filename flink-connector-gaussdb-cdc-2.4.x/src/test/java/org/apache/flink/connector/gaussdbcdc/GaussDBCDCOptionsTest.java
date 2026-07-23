/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.gaussdbcdc;

import org.apache.flink.configuration.Configuration;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** Unit tests for {@link GaussDBCDCOptions}. */
public class GaussDBCDCOptionsTest {

    @Test
    public void testDefaultValues() {
        // Test default values
        assertEquals(Integer.valueOf(8000), GaussDBCDCOptions.PORT.defaultValue());
        assertEquals("public", GaussDBCDCOptions.SCHEMA.defaultValue());
        assertEquals("flink_cdc_slot", GaussDBCDCOptions.SLOT_NAME.defaultValue());
        assertNull(GaussDBCDCOptions.PLUGIN_NAME.defaultValue());
        assertEquals(Boolean.TRUE, GaussDBCDCOptions.SNAPSHOT_MODE.defaultValue());
        assertEquals(Integer.valueOf(1000), GaussDBCDCOptions.CHUNK_SIZE.defaultValue());
        assertEquals(Integer.valueOf(30000), GaussDBCDCOptions.CONNECT_TIMEOUT_MS.defaultValue());
        assertEquals(Integer.valueOf(1000), GaussDBCDCOptions.POLL_INTERVAL_MS.defaultValue());
    }

    @Test
    public void testRequiredOptions() {
        // Test that required options have no default value
        assertNull(GaussDBCDCOptions.HOSTNAME.defaultValue());
        assertNull(GaussDBCDCOptions.DATABASE.defaultValue());
        assertNull(GaussDBCDCOptions.TABLE_NAME.defaultValue());
        assertNull(GaussDBCDCOptions.USERNAME.defaultValue());
        assertNull(GaussDBCDCOptions.PASSWORD.defaultValue());
    }

    @Test
    public void testConfiguration() {
        Configuration config = new Configuration();
        config.setString(GaussDBCDCOptions.HOSTNAME, "localhost");
        config.setInteger(GaussDBCDCOptions.PORT, 5432);
        config.setString(GaussDBCDCOptions.DATABASE, "testdb");
        config.setString(GaussDBCDCOptions.TABLE_NAME, "mytable");
        config.setString(GaussDBCDCOptions.USERNAME, "user");
        config.setString(GaussDBCDCOptions.PASSWORD, "pass");

        assertEquals("localhost", config.get(GaussDBCDCOptions.HOSTNAME));
        assertEquals(5432, config.get(GaussDBCDCOptions.PORT).intValue());
        assertEquals("testdb", config.get(GaussDBCDCOptions.DATABASE));
        assertEquals("mytable", config.get(GaussDBCDCOptions.TABLE_NAME));
        assertEquals("user", config.get(GaussDBCDCOptions.USERNAME));
        assertEquals("pass", config.get(GaussDBCDCOptions.PASSWORD));
    }

    @Test
    public void testOptionKeys() {
        assertEquals("hostname", GaussDBCDCOptions.HOSTNAME.key());
        assertEquals("port", GaussDBCDCOptions.PORT.key());
        assertEquals("database", GaussDBCDCOptions.DATABASE.key());
        assertEquals("table-name", GaussDBCDCOptions.TABLE_NAME.key());
        assertEquals("username", GaussDBCDCOptions.USERNAME.key());
        assertEquals("password", GaussDBCDCOptions.PASSWORD.key());
        assertEquals("slot.name", GaussDBCDCOptions.SLOT_NAME.key());
        assertEquals("snapshot.mode", GaussDBCDCOptions.SNAPSHOT_MODE.key());
    }
}
