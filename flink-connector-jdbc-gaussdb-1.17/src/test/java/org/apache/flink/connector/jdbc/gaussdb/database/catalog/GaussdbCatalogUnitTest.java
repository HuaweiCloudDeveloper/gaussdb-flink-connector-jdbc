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

package org.apache.flink.connector.jdbc.gaussdb.database.catalog;

import org.apache.flink.table.catalog.ObjectPath;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link GaussdbCatalog} methods that don't require a database connection. */
class GaussdbCatalogUnitTest {

    private GaussdbCatalog createCatalog() {
        return new GaussdbCatalog(
                Thread.currentThread().getContextClassLoader(),
                "test_catalog",
                "postgres",
                "user",
                "password",
                "jdbc:gaussdb://localhost:8000/");
    }

    @Test
    void testDefaultDatabase() {
        assertThat(GaussdbCatalog.DEFAULT_DATABASE).isEqualTo("postgres");
    }

    @Test
    void testGetTableName() throws Exception {
        GaussdbCatalog catalog = createCatalog();
        Method method = GaussdbCatalog.class.getDeclaredMethod("getTableName", ObjectPath.class);
        method.setAccessible(true);

        ObjectPath path = new ObjectPath("testdb", "mytable");
        String tableName = (String) method.invoke(catalog, path);
        assertThat(tableName).isEqualTo("mytable");
    }

    @Test
    void testGetTableNameWithSchemaPrefix() throws Exception {
        GaussdbCatalog catalog = createCatalog();
        Method method = GaussdbCatalog.class.getDeclaredMethod("getTableName", ObjectPath.class);
        method.setAccessible(true);

        ObjectPath path = new ObjectPath("testdb", "myschema.mytable");
        String tableName = (String) method.invoke(catalog, path);
        assertThat(tableName).isEqualTo("mytable");
    }

    @Test
    void testGetSchemaName() throws Exception {
        GaussdbCatalog catalog = createCatalog();
        Method method = GaussdbCatalog.class.getDeclaredMethod("getSchemaName", ObjectPath.class);
        method.setAccessible(true);

        ObjectPath path = new ObjectPath("testdb", "mytable");
        String schemaName = (String) method.invoke(catalog, path);
        assertThat(schemaName).isEqualTo("public"); // default schema
    }

    @Test
    void testGetSchemaNameWithSchemaPrefix() throws Exception {
        GaussdbCatalog catalog = createCatalog();
        Method method = GaussdbCatalog.class.getDeclaredMethod("getSchemaName", ObjectPath.class);
        method.setAccessible(true);

        ObjectPath path = new ObjectPath("testdb", "myschema.mytable");
        String schemaName = (String) method.invoke(catalog, path);
        assertThat(schemaName).isEqualTo("myschema");
    }

    @Test
    void testGetSchemaTableName() throws Exception {
        GaussdbCatalog catalog = createCatalog();
        Method method =
                GaussdbCatalog.class.getDeclaredMethod("getSchemaTableName", ObjectPath.class);
        method.setAccessible(true);

        ObjectPath path = new ObjectPath("testdb", "mytable");
        String schemaTableName = (String) method.invoke(catalog, path);
        assertThat(schemaTableName).isEqualTo("public.mytable");
    }

    @Test
    void testGetSchemaTableNameWithSchemaPrefix() throws Exception {
        GaussdbCatalog catalog = createCatalog();
        Method method =
                GaussdbCatalog.class.getDeclaredMethod("getSchemaTableName", ObjectPath.class);
        method.setAccessible(true);

        ObjectPath path = new ObjectPath("testdb", "myschema.mytable");
        String schemaTableName = (String) method.invoke(catalog, path);
        assertThat(schemaTableName).isEqualTo("myschema.mytable");
    }

    @Test
    void testCatalogCreation() {
        GaussdbCatalog catalog = createCatalog();
        assertThat(catalog).isNotNull();
    }

    @Test
    void testGetName() {
        GaussdbCatalog catalog = createCatalog();
        assertThat(catalog.getName()).isEqualTo("test_catalog");
    }

    @Test
    void testGetDefaultDatabase() {
        GaussdbCatalog catalog = createCatalog();
        assertThat(catalog.getDefaultDatabase()).isEqualTo("postgres");
    }
}
