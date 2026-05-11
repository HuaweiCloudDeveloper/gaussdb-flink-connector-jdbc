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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link WalChange}. */
class WalChangeTest {

    @Test
    void testDefaultConstructor() {
        WalChange change = new WalChange();
        assertThat(change).isNotNull();
        assertThat(change.getBeforeColumns()).isNotNull();
        assertThat(change.getAfterColumns()).isNotNull();
    }

    @Test
    void testSetAndGetLsn() {
        WalChange change = new WalChange();
        change.setLsn("0/100");
        assertThat(change.getLsn()).isEqualTo("0/100");
    }

    @Test
    void testSetAndGetXid() {
        WalChange change = new WalChange();
        change.setXid(12345L);
        assertThat(change.getXid()).isEqualTo(12345L);
    }

    @Test
    void testSetAndGetType() {
        WalChange change = new WalChange();
        change.setType(WalChange.ChangeType.INSERT);
        assertThat(change.getType()).isEqualTo(WalChange.ChangeType.INSERT);
    }

    @Test
    void testSetAndGetSchema() {
        WalChange change = new WalChange();
        change.setSchema("public");
        assertThat(change.getSchema()).isEqualTo("public");
    }

    @Test
    void testSetAndGetTable() {
        WalChange change = new WalChange();
        change.setTable("test_table");
        assertThat(change.getTable()).isEqualTo("test_table");
    }

    @Test
    void testSetAndGetBeforeColumns() {
        WalChange change = new WalChange();
        List<WalChange.ColumnValue> before = new ArrayList<>();
        before.add(new WalChange.ColumnValue("id", 23, "1", false));
        before.add(new WalChange.ColumnValue("name", 1043, "old_name", false));
        change.setBeforeColumns(before);

        assertThat(change.getBeforeColumns()).hasSize(2);
        assertThat(change.getBeforeColumns().get(0).getValue()).isEqualTo("1");
    }

    @Test
    void testSetAndGetAfterColumns() {
        WalChange change = new WalChange();
        List<WalChange.ColumnValue> after = new ArrayList<>();
        after.add(new WalChange.ColumnValue("id", 23, "1", false));
        after.add(new WalChange.ColumnValue("name", 1043, "new_name", false));
        change.setAfterColumns(after);

        assertThat(change.getAfterColumns()).hasSize(2);
        assertThat(change.getAfterColumns().get(1).getValue()).isEqualTo("new_name");
    }

    @Test
    void testSetAndGetRawData() {
        WalChange change = new WalChange();
        change.setRawData("raw_wal_data");
        assertThat(change.getRawData()).isEqualTo("raw_wal_data");
    }

    @Test
    void testIsDataChangeForInsert() {
        WalChange change = new WalChange();
        change.setType(WalChange.ChangeType.INSERT);
        assertThat(change.isDataChange()).isTrue();
    }

    @Test
    void testIsDataChangeForUpdate() {
        WalChange change = new WalChange();
        change.setType(WalChange.ChangeType.UPDATE);
        assertThat(change.isDataChange()).isTrue();
    }

    @Test
    void testIsDataChangeForDelete() {
        WalChange change = new WalChange();
        change.setType(WalChange.ChangeType.DELETE);
        assertThat(change.isDataChange()).isTrue();
    }

    @Test
    void testIsDataChangeForBegin() {
        WalChange change = new WalChange();
        change.setType(WalChange.ChangeType.BEGIN);
        assertThat(change.isDataChange()).isFalse();
    }

    @Test
    void testIsDataChangeForCommit() {
        WalChange change = new WalChange();
        change.setType(WalChange.ChangeType.COMMIT);
        assertThat(change.isDataChange()).isFalse();
    }

    @Test
    void testIsDataChangeForUnknown() {
        WalChange change = new WalChange();
        change.setType(WalChange.ChangeType.UNKNOWN);
        assertThat(change.isDataChange()).isFalse();
    }

    @Test
    void testToString() {
        WalChange change = new WalChange();
        change.setLsn("0/100");
        change.setXid(12345L);
        change.setType(WalChange.ChangeType.INSERT);
        change.setSchema("public");
        change.setTable("test_table");

        String str = change.toString();
        assertThat(str).contains("0/100");
        assertThat(str).contains("12345");
        assertThat(str).contains("INSERT");
        assertThat(str).contains("public");
        assertThat(str).contains("test_table");
    }

    @Test
    void testChangeTypeEnumValues() {
        WalChange.ChangeType[] types = WalChange.ChangeType.values();
        assertThat(types).hasSize(6);
        assertThat(types)
                .contains(
                        WalChange.ChangeType.INSERT,
                        WalChange.ChangeType.UPDATE,
                        WalChange.ChangeType.DELETE,
                        WalChange.ChangeType.BEGIN,
                        WalChange.ChangeType.COMMIT,
                        WalChange.ChangeType.UNKNOWN);
    }
}
