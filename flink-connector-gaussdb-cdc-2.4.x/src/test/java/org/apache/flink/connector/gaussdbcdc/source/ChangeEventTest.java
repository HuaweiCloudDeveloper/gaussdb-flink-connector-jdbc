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

package org.apache.flink.connector.gaussdbcdc.source;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Unit tests for {@link ChangeEvent}. */
public class ChangeEventTest {

    @Test
    public void testInsertEvent() {
        RowData row = createRowData(1, "Alice", "F", 20);

        ChangeEvent<RowData> event = ChangeEvent.insert("student", row, System.currentTimeMillis());

        assertTrue(event.isInsert());
        assertFalse(event.isUpdate());
        assertFalse(event.isDelete());
        assertFalse(event.isSnapshot());
        assertEquals("student", event.getTableName());
        assertNull(event.getBefore());
        assertEquals(row, event.getAfter());
    }

    @Test
    public void testUpdateEvent() {
        RowData before = createRowData(1, "Alice", "F", 20);
        RowData after = createRowData(1, "Alice Updated", "F", 21);

        ChangeEvent<RowData> event =
                ChangeEvent.update("student", before, after, System.currentTimeMillis());

        assertFalse(event.isInsert());
        assertTrue(event.isUpdate());
        assertFalse(event.isDelete());
        assertFalse(event.isSnapshot());
        assertEquals("student", event.getTableName());
        assertEquals(before, event.getBefore());
        assertEquals(after, event.getAfter());
    }

    @Test
    public void testDeleteEvent() {
        RowData row = createRowData(1, "Alice", "F", 20);

        ChangeEvent<RowData> event = ChangeEvent.delete("student", row, System.currentTimeMillis());

        assertFalse(event.isInsert());
        assertFalse(event.isUpdate());
        assertTrue(event.isDelete());
        assertFalse(event.isSnapshot());
        assertEquals("student", event.getTableName());
        assertEquals(row, event.getBefore());
        assertNull(event.getAfter());
    }

    @Test
    public void testSnapshotEvent() {
        RowData row = createRowData(1, "Alice", "F", 20);

        ChangeEvent<RowData> event =
                ChangeEvent.snapshot("student", row, System.currentTimeMillis());

        assertFalse(event.isInsert());
        assertFalse(event.isUpdate());
        assertFalse(event.isDelete());
        assertTrue(event.isSnapshot());
        assertEquals("student", event.getTableName());
        assertNull(event.getBefore());
        assertEquals(row, event.getAfter());
    }

    @Test
    public void testChangeEventEquality() {
        RowData row1 = createRowData(1, "Alice", "F", 20);
        RowData row2 = createRowData(1, "Alice", "F", 20);

        ChangeEvent<RowData> event1 = ChangeEvent.insert("student", row1, 1000L);
        ChangeEvent<RowData> event2 = ChangeEvent.insert("student", row2, 1000L);

        assertEquals(event1.getChangeType(), event2.getChangeType());
        assertEquals(event1.getTableName(), event2.getTableName());
    }

    @Test
    public void testChangeEventToString() {
        RowData row = createRowData(1, "Alice", "F", 20);
        ChangeEvent<RowData> event = ChangeEvent.insert("student", row, 1000L);

        String str = event.toString();
        assertNotNull(str);
        assertTrue(str.contains("INSERT"));
        assertTrue(str.contains("student"));
    }

    private RowData createRowData(int id, String name, String gender, int age) {
        GenericRowData row = new GenericRowData(4);
        row.setField(0, id);
        row.setField(1, StringData.fromString(name));
        row.setField(2, StringData.fromString(gender));
        row.setField(3, age);
        return row;
    }
}
