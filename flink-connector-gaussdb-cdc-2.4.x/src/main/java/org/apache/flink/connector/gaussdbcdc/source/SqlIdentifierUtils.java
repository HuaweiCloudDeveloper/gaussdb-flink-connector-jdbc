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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

/** Utilities for safely embedding GaussDB identifiers in SQL text. */
final class SqlIdentifierUtils {

    private SqlIdentifierUtils() {}

    /**
     * Resolve the quote used by the current GaussDB compatibility mode.
     *
     * <p>The GaussDB JDBC driver reports a double quote even for an M-compatible database, while
     * that mode accepts MySQL-style backticks for identifiers. Check the server mode first and use
     * JDBC metadata for other modes.
     */
    static String resolveQuoteString(Connection connection) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            if (stmt != null) {
                try (ResultSet rs = stmt.executeQuery("SHOW sql_compatibility")) {
                    if (rs != null && rs.next() && "M".equalsIgnoreCase(rs.getString(1))) {
                        return "`";
                    }
                }
            }
        } catch (SQLException ignored) {
            // Standard PostgreSQL and some GaussDB variants do not expose sql_compatibility.
        }
        DatabaseMetaData metadata = connection.getMetaData();
        if (metadata == null) {
            return "\"";
        }
        String quote = metadata.getIdentifierQuoteString();
        return quote == null || quote.trim().isEmpty() ? "\"" : quote;
    }

    /** Quote one identifier. Embedded quote characters are escaped per the SQL standard. */
    static String quote(String identifier) {
        return quote(identifier, "\"");
    }

    static String quote(String identifier, String quoteString) {
        if (identifier == null) {
            throw new IllegalArgumentException("SQL identifier must not be null");
        }
        if (quoteString == null || quoteString.isEmpty()) {
            throw new IllegalArgumentException("SQL identifier quote must not be empty");
        }
        return quoteString
                + identifier.replace(quoteString, quoteString + quoteString)
                + quoteString;
    }

    /** Quote a schema-qualified table name, treating both parts as separate identifiers. */
    static String qualified(String schema, String table) {
        return qualified(schema, table, "\"");
    }

    static String qualified(String schema, String table, String quoteString) {
        return quote(schema, quoteString) + "." + quote(table, quoteString);
    }

    /** Quote each column separately before building a SELECT list. */
    static String columnList(List<String> columns) {
        return columnList(columns, "\"");
    }

    static String columnList(List<String> columns, String quoteString) {
        return columns.stream()
                .map(column -> quote(column, quoteString))
                .collect(Collectors.joining(", "));
    }
}
