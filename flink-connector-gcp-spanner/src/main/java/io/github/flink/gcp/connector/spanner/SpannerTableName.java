/*
 * Copyright 2026 The flink-gcp authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.flink.gcp.connector.spanner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;

import com.google.cloud.spanner.Dialect;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Locale;
import java.util.Objects;

/** A table name as rendered for Spanner APIs and compared with {@code INFORMATION_SCHEMA}. */
@Internal
public final class SpannerTableName implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Dialect dialect;
    private final boolean explicitlyQualified;
    private final String schema;
    private final String table;
    private final String apiName;

    private SpannerTableName(
            Dialect dialect,
            boolean explicitlyQualified,
            String schema,
            String table,
            String apiName) {
        this.dialect = dialect;
        this.explicitlyQualified = explicitlyQualified;
        this.schema = schema;
        this.table = table;
        this.apiName = apiName;
    }

    /** Creates a name from the Table API options. */
    public static SpannerTableName of(@Nullable String schema, String table, Dialect dialect) {
        if (schema == null) {
            return new SpannerTableName(
                    dialect, false, dialect == Dialect.POSTGRESQL ? "public" : "", table, table);
        }
        String schemaName = SpannerIdentifier.configured(schema, dialect, "schema");
        String tableName = SpannerIdentifier.configured(table, dialect, "table");
        return new SpannerTableName(
                dialect, true, schemaName, tableName, schemaName + "." + tableName);
    }

    /** Returns the name passed to Spanner data APIs. */
    public String apiName() {
        return apiName;
    }

    /** Returns the schema spelling exposed by {@code INFORMATION_SCHEMA}. */
    public String schema() {
        return schema;
    }

    /** Returns the table spelling exposed by {@code INFORMATION_SCHEMA}. */
    public String table() {
        return table;
    }

    /** Returns whether a native API table name identifies this table. */
    public boolean matchesNativeApiName(String nativeApiName) {
        String expected =
                explicitlyQualified
                        ? catalogKey(schema, table, dialect)
                        : nativeApiKey(apiName, dialect);
        return expected.equals(nativeApiKey(nativeApiName, dialect));
    }

    /** Resolves an unqualified access-path option in this table's schema. */
    public AccessPathName accessPath(String value, String option) {
        if (!explicitlyQualified) {
            if (value.trim().isEmpty()) {
                throw new ValidationException(option + " must not be blank.");
            }
            return new AccessPathName(value, value);
        }
        String name = SpannerIdentifier.configured(value, dialect, option);
        return new AccessPathName(schema + "." + name, name);
    }

    /** Returns a dialect-aware key for a catalog row. */
    public static String catalogKey(String schema, String table, Dialect dialect) {
        if (dialect == Dialect.GOOGLE_STANDARD_SQL) {
            return fold(schema) + '\u0000' + fold(table);
        }
        return schema + '\u0000' + table;
    }

    /** Returns a dialect-aware key for a table name carried by a mutation. */
    public static String nativeApiKey(String apiName, Dialect dialect) {
        if (apiName == null || apiName.trim().isEmpty()) {
            throw new IllegalArgumentException("The native API table name is blank.");
        }
        int separator = apiName.indexOf('.');
        if (separator < 0) {
            String defaultSchema = dialect == Dialect.POSTGRESQL ? "public" : "";
            return catalogKey(defaultSchema, apiName, dialect);
        }
        if (separator == 0
                || separator == apiName.length() - 1
                || apiName.indexOf('.', separator + 1) >= 0) {
            throw new IllegalArgumentException(
                    "The native API table name is not an unqualified name or schema.table.");
        }
        return catalogKey(
                apiName.substring(0, separator), apiName.substring(separator + 1), dialect);
    }

    /** Returns a dialect-aware key for one identifier from a catalog row. */
    public static String catalogIdentifierKey(String value, Dialect dialect) {
        return dialect == Dialect.GOOGLE_STANDARD_SQL ? fold(value) : value;
    }

    private static String fold(String value) {
        return value.toLowerCase(Locale.ROOT);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SpannerTableName)) {
            return false;
        }
        SpannerTableName that = (SpannerTableName) other;
        return explicitlyQualified == that.explicitlyQualified
                && dialect == that.dialect
                && schema.equals(that.schema)
                && table.equals(that.table)
                && apiName.equals(that.apiName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dialect, explicitlyQualified, schema, table, apiName);
    }

    /** The name of one table-local access path, such as an index. */
    @Internal
    public static final class AccessPathName implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String apiName;
        private final String catalogName;

        private AccessPathName(String apiName, String catalogName) {
            this.apiName = apiName;
            this.catalogName = catalogName;
        }

        public String apiName() {
            return apiName;
        }

        public String catalogName() {
            return catalogName;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof AccessPathName)) {
                return false;
            }
            AccessPathName that = (AccessPathName) other;
            return apiName.equals(that.apiName) && catalogName.equals(that.catalogName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(apiName, catalogName);
        }
    }
}
