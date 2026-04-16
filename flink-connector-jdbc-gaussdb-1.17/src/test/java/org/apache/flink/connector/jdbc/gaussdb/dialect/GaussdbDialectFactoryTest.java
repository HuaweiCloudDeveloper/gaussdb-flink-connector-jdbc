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

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectFactory;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link GaussdbDialectFactory}. */
class GaussdbDialectFactoryTest {

    @Test
    void testFactoryAcceptsGaussdbURL() {
        GaussdbDialectFactory factory = new GaussdbDialectFactory();
        assertThat(factory.acceptsURL("jdbc:gaussdb://localhost:5432/test")).isTrue();
    }

    @Test
    void testFactoryAcceptsGaussdbURLWithPort() {
        GaussdbDialectFactory factory = new GaussdbDialectFactory();
        assertThat(factory.acceptsURL("jdbc:gaussdb://localhost:8000/mydb")).isTrue();
    }

    @Test
    void testFactoryAcceptsGaussdbURLWithIP() {
        GaussdbDialectFactory factory = new GaussdbDialectFactory();
        assertThat(factory.acceptsURL("jdbc:gaussdb://192.168.1.100:5432/test")).isTrue();
    }

    @Test
    void testFactoryRejectsPostgresURL() {
        GaussdbDialectFactory factory = new GaussdbDialectFactory();
        assertThat(factory.acceptsURL("jdbc:postgresql://localhost:5432/test")).isFalse();
    }

    @Test
    void testFactoryRejectsMySQLURL() {
        GaussdbDialectFactory factory = new GaussdbDialectFactory();
        assertThat(factory.acceptsURL("jdbc:mysql://localhost:3306/test")).isFalse();
    }

    @Test
    void testFactoryRejectsOracleURL() {
        GaussdbDialectFactory factory = new GaussdbDialectFactory();
        assertThat(factory.acceptsURL("jdbc:oracle:thin:@localhost:1521:xe")).isFalse();
    }

    @Test
    void testFactoryRejectsSQLServerURL() {
        GaussdbDialectFactory factory = new GaussdbDialectFactory();
        assertThat(factory.acceptsURL("jdbc:sqlserver://localhost:1433;databaseName=test"))
                .isFalse();
    }

    @Test
    void testFactoryRejectsEmptyURL() {
        GaussdbDialectFactory factory = new GaussdbDialectFactory();
        assertThat(factory.acceptsURL("")).isFalse();
    }

    @Test
    void testFactoryRejectsNullURL() {
        GaussdbDialectFactory factory = new GaussdbDialectFactory();
        // Null URL should throw NullPointerException or return false
        try {
            boolean result = factory.acceptsURL(null);
            assertThat(result).isFalse();
        } catch (NullPointerException e) {
            // Expected behavior
        }
    }

    @Test
    void testFactoryCreatesGaussdbDialect() {
        GaussdbDialectFactory factory = new GaussdbDialectFactory();
        JdbcDialect dialect = factory.create();
        assertThat(dialect).isNotNull();
        assertThat(dialect).isInstanceOf(GaussdbDialect.class);
    }

    @Test
    void testFactoryCreatesDialectWithCorrectName() {
        GaussdbDialectFactory factory = new GaussdbDialectFactory();
        JdbcDialect dialect = factory.create();
        assertThat(dialect.dialectName()).isEqualTo("GaussDB");
    }

    @Test
    void testFactoryImplementsJdbcDialectFactory() {
        GaussdbDialectFactory factory = new GaussdbDialectFactory();
        assertThat(factory).isInstanceOf(JdbcDialectFactory.class);
    }

    @Test
    void testFactoryCreatesNewInstanceEachTime() {
        GaussdbDialectFactory factory = new GaussdbDialectFactory();
        JdbcDialect dialect1 = factory.create();
        JdbcDialect dialect2 = factory.create();
        assertThat(dialect1).isNotSameAs(dialect2);
        // Both should be GaussdbDialect instances with same name
        assertThat(dialect1.dialectName()).isEqualTo(dialect2.dialectName());
    }
}
