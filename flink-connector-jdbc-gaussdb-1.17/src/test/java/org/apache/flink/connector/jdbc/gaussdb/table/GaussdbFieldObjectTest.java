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

package org.apache.flink.connector.jdbc.gaussdb.table;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GaussdbFieldObject}. */
class GaussdbFieldObjectTest {

    @Test
    void testConstructorAndGetters() {
        GaussdbFieldObject field = new GaussdbFieldObject(3, "test_value");
        assertThat(field.getIndex()).isEqualTo(3);
        assertThat(field.getValue()).isEqualTo("test_value");
    }

    @Test
    void testWithNullValue() {
        GaussdbFieldObject field = new GaussdbFieldObject(0, null);
        assertThat(field.getIndex()).isEqualTo(0);
        assertThat(field.getValue()).isNull();
    }

    @Test
    void testWithIntegerValue() {
        GaussdbFieldObject field = new GaussdbFieldObject(1, 42);
        assertThat(field.getIndex()).isEqualTo(1);
        assertThat(field.getValue()).isEqualTo(42);
    }

    @Test
    void testWithDoubleValue() {
        GaussdbFieldObject field = new GaussdbFieldObject(2, 3.14);
        assertThat(field.getIndex()).isEqualTo(2);
        assertThat(field.getValue()).isEqualTo(3.14);
    }
}
