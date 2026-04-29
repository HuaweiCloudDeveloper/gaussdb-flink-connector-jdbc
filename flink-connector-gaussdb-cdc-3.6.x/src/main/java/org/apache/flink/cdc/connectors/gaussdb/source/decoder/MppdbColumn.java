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

package org.apache.flink.cdc.connectors.gaussdb.source.decoder;

import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource;
import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.connection.ReplicationMessage.Column;
import io.debezium.connector.postgresql.connection.ReplicationMessage.ColumnTypeMetadata;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Shared {@link Column} implementation for mppdb_decoding output.
 *
 * <p>Used by both JSON and binary decoders. Supports construction from either a type name string
 * (JSON format) or a type OID integer (binary format).
 */
public class MppdbColumn implements Column {

    private static final ColumnTypeMetadata EMPTY_COLUMN_TYPE_METADATA =
            new ColumnTypeMetadata() {
                @Override
                public int getLength() {
                    return -1;
                }

                @Override
                public int getScale() {
                    return -1;
                }
            };

    private final String name;
    private final String typeName;
    private final int typeOid;
    private final String value;
    private final boolean isNull;

    /** Constructs a column from a type name string (JSON format). */
    MppdbColumn(String name, String typeName, String value, boolean isNull) {
        this.name = name;
        this.typeName = typeName;
        this.typeOid = mapTypeNameToOid(typeName);
        this.value = value;
        this.isNull = isNull;
    }

    /** Constructs a column from a type OID integer (binary format). */
    MppdbColumn(String name, int typeOid, String value, boolean isNull) {
        this.name = name;
        this.typeOid = typeOid;
        this.typeName = mapOidToTypeName(typeOid);
        this.value = value;
        this.isNull = isNull;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public PostgresType getType() {
        return newPostgresType(typeName, typeOid);
    }

    @Override
    public ColumnTypeMetadata getTypeMetadata() {
        return EMPTY_COLUMN_TYPE_METADATA;
    }

    @Override
    public Object getValue(
            PostgresStreamingChangeEventSource.PgConnectionSupplier connection,
            boolean includeUnknownDatatypes) {
        if (isNull) {
            return null;
        }
        return value;
    }

    @Override
    public boolean isOptional() {
        return true;
    }

    /** Creates a PostgresType using Unsafe to bypass the private constructor. */
    private static PostgresType newPostgresType(String typeName, int oid) {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            Unsafe unsafe = (Unsafe) f.get(null);
            PostgresType pt = (PostgresType) unsafe.allocateInstance(PostgresType.class);
            setField(unsafe, pt, "name", typeName);
            setField(unsafe, pt, "oid", oid);
            setField(unsafe, pt, "arrayOid", -1);
            setField(unsafe, pt, "length", -1);
            setField(unsafe, pt, "scale", -1);
            setField(unsafe, pt, "optional", true);
            return pt;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create PostgresType", e);
        }
    }

    private static void setField(Unsafe unsafe, Object obj, String fieldName, Object value)
            throws Exception {
        Field field = PostgresType.class.getDeclaredField(fieldName);
        unsafe.putObject(obj, unsafe.objectFieldOffset(field), value);
    }

    private static void setField(Unsafe unsafe, Object obj, String fieldName, int value)
            throws Exception {
        Field field = PostgresType.class.getDeclaredField(fieldName);
        unsafe.putInt(obj, unsafe.objectFieldOffset(field), value);
    }

    /** Maps a type name string to a PostgreSQL type OID. */
    static int mapTypeNameToOid(String typeName) {
        if (typeName == null) {
            return 25;
        }
        switch (typeName.toLowerCase()) {
            case "integer":
            case "int":
            case "int4":
                return 23;
            case "bigint":
            case "int8":
                return 20;
            case "smallint":
            case "int2":
                return 21;
            case "boolean":
            case "bool":
                return 16;
            case "real":
            case "float4":
                return 700;
            case "double precision":
            case "float8":
                return 701;
            case "numeric":
            case "decimal":
                return 1700;
            case "character varying":
            case "varchar":
                return 1043;
            case "character":
            case "char":
                return 1042;
            case "text":
                return 25;
            case "timestamp without time zone":
            case "timestamp":
                return 1114;
            case "timestamp with time zone":
            case "timestamptz":
                return 1184;
            case "date":
                return 1082;
            default:
                return 25;
        }
    }

    /** Maps a PostgreSQL type OID to a type name string. */
    static String mapOidToTypeName(int oid) {
        switch (oid) {
            case 23:
                return "int4";
            case 20:
                return "int8";
            case 21:
                return "int2";
            case 16:
                return "bool";
            case 700:
                return "float4";
            case 701:
                return "float8";
            case 1700:
                return "numeric";
            case 1043:
                return "varchar";
            case 1042:
                return "bpchar";
            case 25:
                return "text";
            case 1114:
                return "timestamp";
            case 1184:
                return "timestamptz";
            case 1082:
                return "date";
            case 17:
                return "bytea";
            case 114:
                return "json";
            case 199:
                return "jsonb";
            case 2950:
                return "uuid";
            case 18:
                return "char";
            case 26:
                return "oid";
            default:
                return "text";
        }
    }
}
