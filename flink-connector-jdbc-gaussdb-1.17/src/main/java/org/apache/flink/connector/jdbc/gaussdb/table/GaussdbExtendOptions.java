/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.gaussdb.table;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.Objects;

/** JDBC sink batch options. */
@PublicEvolving
public class GaussdbExtendOptions implements Serializable {

    private final boolean ignoreNullWhenUpdate;

    private GaussdbExtendOptions(boolean ignoreNullWhenUpdate) {
        this.ignoreNullWhenUpdate = ignoreNullWhenUpdate;
    }

    public boolean isIgnoreNullWhenUpdate() {
        return ignoreNullWhenUpdate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GaussdbExtendOptions that = (GaussdbExtendOptions) o;
        return ignoreNullWhenUpdate == that.ignoreNullWhenUpdate;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ignoreNullWhenUpdate);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static GaussdbExtendOptions defaults() {
        return builder().build();
    }

    /** Builder for {@link GaussdbExtendOptions}. */
    @PublicEvolving
    public static final class Builder {
        private boolean ignoreNullWhenUpdate = false;

        public Builder withIgnoreNullWhenUpdate(boolean ignoreNullWhenUpdate) {
            this.ignoreNullWhenUpdate = ignoreNullWhenUpdate;
            return this;
        }

        public GaussdbExtendOptions build() {
            return new GaussdbExtendOptions(ignoreNullWhenUpdate);
        }
    }
}
