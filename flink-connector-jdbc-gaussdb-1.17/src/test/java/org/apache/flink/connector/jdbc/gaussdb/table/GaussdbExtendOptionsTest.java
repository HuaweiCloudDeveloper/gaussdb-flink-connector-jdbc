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

/** Tests for {@link GaussdbExtendOptions}. */
class GaussdbExtendOptionsTest {

    @Test
    void testDefaults() {
        GaussdbExtendOptions options = GaussdbExtendOptions.defaults();
        assertThat(options.isIgnoreNullWhenUpdate()).isFalse();
    }

    @Test
    void testBuilderWithIgnoreNullTrue() {
        GaussdbExtendOptions options =
                GaussdbExtendOptions.builder().withIgnoreNullWhenUpdate(true).build();
        assertThat(options.isIgnoreNullWhenUpdate()).isTrue();
    }

    @Test
    void testBuilderWithIgnoreNullFalse() {
        GaussdbExtendOptions options =
                GaussdbExtendOptions.builder().withIgnoreNullWhenUpdate(false).build();
        assertThat(options.isIgnoreNullWhenUpdate()).isFalse();
    }

    @Test
    void testEqualsSameValues() {
        GaussdbExtendOptions options1 =
                GaussdbExtendOptions.builder().withIgnoreNullWhenUpdate(true).build();
        GaussdbExtendOptions options2 =
                GaussdbExtendOptions.builder().withIgnoreNullWhenUpdate(true).build();
        assertThat(options1).isEqualTo(options2);
        assertThat(options1.hashCode()).isEqualTo(options2.hashCode());
    }

    @Test
    void testEqualsDifferentValues() {
        GaussdbExtendOptions options1 =
                GaussdbExtendOptions.builder().withIgnoreNullWhenUpdate(true).build();
        GaussdbExtendOptions options2 =
                GaussdbExtendOptions.builder().withIgnoreNullWhenUpdate(false).build();
        assertThat(options1).isNotEqualTo(options2);
    }

    @Test
    void testEqualsSameObject() {
        GaussdbExtendOptions options = GaussdbExtendOptions.defaults();
        assertThat(options).isEqualTo(options);
    }

    @Test
    void testEqualsNull() {
        GaussdbExtendOptions options = GaussdbExtendOptions.defaults();
        assertThat(options).isNotEqualTo(null);
    }

    @Test
    void testEqualsDifferentType() {
        GaussdbExtendOptions options = GaussdbExtendOptions.defaults();
        assertThat(options).isNotEqualTo("string");
    }
}
