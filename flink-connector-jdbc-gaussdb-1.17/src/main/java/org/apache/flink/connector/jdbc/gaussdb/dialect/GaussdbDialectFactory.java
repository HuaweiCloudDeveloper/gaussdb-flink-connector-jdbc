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

package org.apache.flink.connector.jdbc.gaussdb.dialect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectFactory;

/**
 * Factory for GaussdbDialect.
 *
 * <p>Supports two URL schemes depending on which GaussDB JDBC driver is deployed:
 * <ul>
 *   <li>jdbc:gaussdb:// - used by gaussdbjdbc.jar (com.huawei.gaussdb.jdbc.Driver)
 *   <li>jdbc:postgresql:// - used by gsjdbc4.jar (org.postgresql.Driver)
 * </ul>
 */
@Internal
public class GaussdbDialectFactory implements JdbcDialectFactory {

    @Override
    public boolean acceptsURL(String url) {
        // Always accept jdbc:gaussdb: (gaussdbjdbc.jar driver)
        if (url.startsWith("jdbc:gaussdb:")) {
            return true;
        }
        // Only accept jdbc:postgresql: when a GaussDB driver is actually on the
        // classpath. This avoids conflicts with the upstream PostgresDialectFactory
        // when the user is connecting to a real PostgreSQL database.
        if (url.startsWith("jdbc:postgresql:")) {
            try {
                Class.forName("com.huawei.gaussdb.jdbc.Driver");
                return true;
            } catch (ClassNotFoundException e) {
                // gaussdbjdbc not found — try gsjdbc4 (org.postgresql.Driver)
                // gsjdbc4 is a GaussDB fork of pgjdbc, distinguish it from
                // upstream postgresql.jar by checking for GaussDB-specific class
                try {
                    Class.forName("com.huawei.gaussdb.jdbc.util.PSQLException");
                    return true;
                } catch (ClassNotFoundException e2) {
                    // No GaussDB driver present — let upstream PostgresDialectFactory handle it
                    return false;
                }
            }
        }
        return false;
    }

    @Override
    public JdbcDialect create() {
        return new GaussdbDialect();
    }
}
