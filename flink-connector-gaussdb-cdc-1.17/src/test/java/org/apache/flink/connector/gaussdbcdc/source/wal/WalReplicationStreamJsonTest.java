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

package org.apache.flink.connector.gaussdbcdc.source.wal;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for JSON parsing and internal methods in {@link WalReplicationStream}. */
class WalReplicationStreamJsonTest {

    @Test
    void testParseJsonInsert() throws Exception {
        String json =
                "{\"table_name\":\"public.student\",\"op_type\":\"INSERT\","
                        + "\"columns_name\":[\"id\",\"name\",\"age\"],"
                        + "\"columns_type\":[\"integer\",\"character varying\",\"integer\"],"
                        + "\"columns_val\":[\"1\",\"'Alice'\",\"25\"],"
                        + "\"old_keys_name\":[],\"old_keys_type\":[],\"old_keys_val\":[]}";

        WalChange change = invokeParseChangeData("0/1", 100, json);
        assertThat(change.getType()).isEqualTo(WalChange.ChangeType.INSERT);
        assertThat(change.getSchema()).isEqualTo("public");
        assertThat(change.getTable()).isEqualTo("student");
        assertThat(change.getAfterColumns()).hasSize(3);
        assertThat(change.getAfterColumns().get(0).getColumnName()).isEqualTo("id");
        assertThat(change.getAfterColumns().get(0).getValue()).isEqualTo("1");
        assertThat(change.getAfterColumns().get(1).getValue()).isEqualTo("Alice");
        assertThat(change.getAfterColumns().get(2).getValue()).isEqualTo("25");
    }

    @Test
    void testParseJsonUpdate() throws Exception {
        String json =
                "{\"table_name\":\"public.student\",\"op_type\":\"UPDATE\","
                        + "\"columns_name\":[\"id\",\"name\",\"age\"],"
                        + "\"columns_type\":[\"integer\",\"character varying\",\"integer\"],"
                        + "\"columns_val\":[\"1\",\"'Alice'\",\"26\"],"
                        + "\"old_keys_name\":[\"id\"],"
                        + "\"old_keys_type\":[\"integer\"],"
                        + "\"old_keys_val\":[\"1\"]}";

        WalChange change = invokeParseChangeData("0/2", 101, json);
        assertThat(change.getType()).isEqualTo(WalChange.ChangeType.UPDATE);
        assertThat(change.getAfterColumns()).hasSize(3);
        assertThat(change.getAfterColumns().get(2).getValue()).isEqualTo("26");
        assertThat(change.getBeforeColumns()).hasSize(1);
        assertThat(change.getBeforeColumns().get(0).getColumnName()).isEqualTo("id");
    }

    @Test
    void testParseJsonDelete() throws Exception {
        String json =
                "{\"table_name\":\"public.student\",\"op_type\":\"DELETE\","
                        + "\"columns_name\":[\"id\",\"name\"],"
                        + "\"columns_type\":[\"integer\",\"character varying\"],"
                        + "\"columns_val\":[\"5\",\"'Bob'\"],"
                        + "\"old_keys_name\":[\"id\"],"
                        + "\"old_keys_type\":[\"integer\"],"
                        + "\"old_keys_val\":[\"5\"]}";

        WalChange change = invokeParseChangeData("0/3", 102, json);
        assertThat(change.getType()).isEqualTo(WalChange.ChangeType.DELETE);
        assertThat(change.getBeforeColumns()).hasSize(1);
        assertThat(change.getBeforeColumns().get(0).getValue()).isEqualTo("5");
    }

    @Test
    void testParseCommitWithCsn() throws Exception {
        String text = "COMMIT 100 (at 2024-01-15 10:30:00.089882+08) CSN 99243";
        WalChange change = invokeParseChangeData("0/4", 100, text);
        assertThat(change.getType()).isEqualTo(WalChange.ChangeType.COMMIT);
        assertThat(change.getCsn()).isEqualTo(99243L);
    }

    @Test
    void testParseBegin() throws Exception {
        String text = "BEGIN 100";
        WalChange change = invokeParseChangeData("0/5", 100, text);
        assertThat(change.getType()).isEqualTo(WalChange.ChangeType.BEGIN);
    }

    @Test
    void testParseTextInsert() throws Exception {
        String text = "INSERT: public.test [id[integer]:1 name[character varying]:'Alice']";
        WalChange change = invokeParseChangeData("0/6", 103, text);
        assertThat(change.getType()).isEqualTo(WalChange.ChangeType.INSERT);
        assertThat(change.getSchema()).isEqualTo("public");
        assertThat(change.getTable()).isEqualTo("test");
    }

    @Test
    void testParseTextUpdate() throws Exception {
        String text = "UPDATE: public.test [id[integer]:1 name[character varying]:'Bob']";
        WalChange change = invokeParseChangeData("0/7", 104, text);
        assertThat(change.getType()).isEqualTo(WalChange.ChangeType.UPDATE);
    }

    @Test
    void testParseTextDelete() throws Exception {
        String text = "DELETE: public.test [id[integer]:5]";
        WalChange change = invokeParseChangeData("0/8", 105, text);
        assertThat(change.getType()).isEqualTo(WalChange.ChangeType.DELETE);
    }

    @Test
    void testParseUnknownData() throws Exception {
        String text = "SOME UNKNOWN DATA";
        WalChange change = invokeParseChangeData("0/9", 106, text);
        assertThat(change.getType()).isEqualTo(WalChange.ChangeType.UNKNOWN);
    }

    @Test
    void testParseJsonWithNoSchema() throws Exception {
        String json =
                "{\"table_name\":\"my_table\",\"op_type\":\"INSERT\","
                        + "\"columns_name\":[\"id\"],"
                        + "\"columns_type\":[\"integer\"],"
                        + "\"columns_val\":[\"1\"],"
                        + "\"old_keys_name\":[],\"old_keys_type\":[],\"old_keys_val\":[]}";
        WalChange change = invokeParseChangeData("0/A", 107, json);
        assertThat(change.getType()).isEqualTo(WalChange.ChangeType.INSERT);
        assertThat(change.getTable()).isEqualTo("my_table");
        assertThat(change.getSchema()).isNull();
    }

    @Test
    void testParseJsonWithNullValue() throws Exception {
        String json =
                "{\"table_name\":\"public.t1\",\"op_type\":\"INSERT\","
                        + "\"columns_name\":[\"id\",\"val\"],"
                        + "\"columns_type\":[\"integer\",\"character varying\"],"
                        + "\"columns_val\":[\"1\",\"null\"],"
                        + "\"old_keys_name\":[],\"old_keys_type\":[],\"old_keys_val\":[]}";
        WalChange change = invokeParseChangeData("0/B", 108, json);
        assertThat(change.getAfterColumns().get(1).isNull()).isTrue();
        assertThat(change.getAfterColumns().get(1).getValue()).isNull();
    }

    @Test
    void testMapTypeNameToOid() throws Exception {
        assertThat(invokeMapTypeNameToOid("integer")).isEqualTo(23);
        assertThat(invokeMapTypeNameToOid("bigint")).isEqualTo(20);
        assertThat(invokeMapTypeNameToOid("smallint")).isEqualTo(21);
        assertThat(invokeMapTypeNameToOid("boolean")).isEqualTo(16);
        assertThat(invokeMapTypeNameToOid("real")).isEqualTo(700);
        assertThat(invokeMapTypeNameToOid("double precision")).isEqualTo(701);
        assertThat(invokeMapTypeNameToOid("numeric")).isEqualTo(1700);
        assertThat(invokeMapTypeNameToOid("character varying")).isEqualTo(1043);
        assertThat(invokeMapTypeNameToOid("character")).isEqualTo(1042);
        assertThat(invokeMapTypeNameToOid("text")).isEqualTo(25);
        assertThat(invokeMapTypeNameToOid("timestamp without time zone")).isEqualTo(1114);
        assertThat(invokeMapTypeNameToOid("timestamp with time zone")).isEqualTo(1184);
        assertThat(invokeMapTypeNameToOid("date")).isEqualTo(1082);
        assertThat(invokeMapTypeNameToOid("unknown_type")).isEqualTo(25); // fallback
        assertThat(invokeMapTypeNameToOid(null)).isEqualTo(25); // null fallback
    }

    @Test
    void testBuildSlotOptionsParallel() throws Exception {
        java.sql.Connection conn =
                new WalReplicationStreamJsonTest().createMockConnectionForBuildOptions();
        WalReplicationStream stream =
                new WalReplicationStream(conn, "slot", "mppdb_decoding", 4, "b", true, 1000);
        java.util.Properties props = invokeBuildSlotOptions(stream);
        assertThat(props.getProperty("parallel-decode-num")).isEqualTo("4");
        assertThat(props.getProperty("decode-style")).isEqualTo("b");
        assertThat(props.getProperty("sending-batch")).isEqualTo("1");
        assertThat(props.getProperty("include-xids")).isEqualTo("1");
        assertThat(props.getProperty("include-timestamp")).isEqualTo("1");
    }

    @Test
    void testBuildSlotOptionsSerial() throws Exception {
        java.sql.Connection conn =
                new WalReplicationStreamJsonTest().createMockConnectionForBuildOptions();
        WalReplicationStream stream =
                new WalReplicationStream(conn, "slot", "mppdb_decoding", 1, "b", false, 1000);
        java.util.Properties props = invokeBuildSlotOptions(stream);
        assertThat(props.getProperty("parallel-decode-num")).isNull();
        assertThat(props.getProperty("decode-style")).isNull();
        assertThat(props.getProperty("include-xids")).isEqualTo("1");
    }

    @Test
    void testIsUseReplicationApi() {
        java.sql.Connection conn = createMockConnectionForBuildOptions();
        WalReplicationStream stream =
                new WalReplicationStream(conn, "slot", "mppdb_decoding", 1, "b", false, 1000);
        assertThat(stream.isUseReplicationApi()).isFalse();
    }

    private java.sql.Connection createMockConnectionForBuildOptions() {
        return org.mockito.Mockito.mock(java.sql.Connection.class);
    }

    // --- Reflection helpers ---

    private WalChange invokeParseChangeData(String lsn, long xid, String data) throws Exception {
        java.sql.Connection conn = org.mockito.Mockito.mock(java.sql.Connection.class);
        WalReplicationStream stream =
                new WalReplicationStream(conn, "slot", "mppdb_decoding", 1, "b", false, 1000);
        java.lang.reflect.Method method =
                WalReplicationStream.class.getDeclaredMethod(
                        "parseChangeData", String.class, long.class, String.class);
        method.setAccessible(true);
        return (WalChange) method.invoke(stream, lsn, xid, data);
    }

    private int invokeMapTypeNameToOid(String typeName) throws Exception {
        java.sql.Connection conn = org.mockito.Mockito.mock(java.sql.Connection.class);
        WalReplicationStream stream =
                new WalReplicationStream(conn, "slot", "mppdb_decoding", 1, "b", false, 1000);
        java.lang.reflect.Method method =
                WalReplicationStream.class.getDeclaredMethod("mapTypeNameToOid", String.class);
        method.setAccessible(true);
        return (int) method.invoke(stream, typeName);
    }

    private java.util.Properties invokeBuildSlotOptions(WalReplicationStream stream)
            throws Exception {
        java.lang.reflect.Method method =
                WalReplicationStream.class.getDeclaredMethod("buildSlotOptions");
        method.setAccessible(true);
        return (java.util.Properties) method.invoke(stream);
    }
}
