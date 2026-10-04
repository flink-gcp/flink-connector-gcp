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

import com.google.cloud.spanner.Dialect;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A table's schema and name as one Flink object name: the default schema's tables unqualified, a
 * named schema's as {@code schema.table}, each part in the dialect's canonical quoting.
 *
 * <p>The name is Flink's: an {@code ObjectPath} holds a database name and an object name, and this
 * class is the object name the {@code spanner} catalog gives a table. It converts, in both
 * directions, between that object name and the native schema and table names {@code
 * INFORMATION_SCHEMA} reports. It is not {@link SpannerTableName}, which decodes the connector's
 * {@code named-schema} and {@code table} options of one configured table into the names the data
 * APIs take; the catalog emits those options through {@link #encodePart(String, Dialect)}.
 *
 * <p>Spanner addresses a table by database, schema and table, while a Flink catalog has two levels
 * below its name, so the schema travels inside the Flink object name, as the JDBC connector's
 * PostgreSQL catalog does. {@link #format(String, String, Dialect)} and {@link #parse(String,
 * Dialect)} are inverses: every name {@code format} produces parses back to the same native schema
 * and table, which is what lets a name {@code SHOW TABLES} printed resolve through {@code
 * getTable}. A part is quoted when its bare spelling would decode to a different name: a PostgreSQL
 * name with an upper-case letter, which an unquoted part folds, or any name outside the dialect's
 * plain identifier grammar.
 */
@Internal
public final class SpannerObjectName {

    private final String schema;
    private final String table;

    private SpannerObjectName(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    /**
     * Creates a name from native schema and table names, as {@code INFORMATION_SCHEMA} spells them.
     *
     * @param schema the native schema name; {@link Dialect#getDefaultSchema()} for the default one
     * @param table the native table name
     * @return the name
     */
    public static SpannerObjectName of(String schema, String table) {
        return new SpannerObjectName(schema, table);
    }

    /**
     * Renders a table as a Flink object name.
     *
     * @param schema the native schema name, as {@code INFORMATION_SCHEMA} spells it
     * @param table the native table name
     * @param dialect the database's dialect
     * @return the table alone for the default schema, otherwise {@code schema.table}
     */
    public static String format(String schema, String table, Dialect dialect) {
        String formattedTable = SpannerIdentifier.encode(table, dialect);
        return schema.equals(dialect.getDefaultSchema())
                ? formattedTable
                : SpannerIdentifier.encode(schema, dialect) + "." + formattedTable;
    }

    /**
     * Renders one native name as a component in canonical quoting, the form the connector's {@code
     * schema} and {@code table} options decode.
     *
     * @param nativeName the native name
     * @param dialect the database's dialect
     * @return the name, quoted where its bare spelling would decode to a different one
     */
    public static String encodePart(String nativeName, Dialect dialect) {
        return SpannerIdentifier.encode(nativeName, dialect);
    }

    /**
     * Parses a Flink object name into a native schema and table.
     *
     * @param objectName the name as Flink passes it, after its own identifier quoting is removed
     * @param dialect the database's dialect
     * @return the name, or empty when it is not one or two parts in canonical quoting, which no
     *     table can be named
     */
    public static Optional<SpannerObjectName> parse(String objectName, Dialect dialect) {
        List<String> parts = split(objectName, SpannerIdentifier.quote(dialect));
        if (parts == null || parts.isEmpty() || parts.size() > 2) {
            return Optional.empty();
        }
        List<String> decoded = new ArrayList<>(parts.size());
        for (String part : parts) {
            try {
                decoded.add(SpannerIdentifier.decode(part, dialect));
            } catch (IllegalArgumentException e) {
                return Optional.empty();
            }
        }
        return Optional.of(
                decoded.size() == 1
                        ? new SpannerObjectName(dialect.getDefaultSchema(), decoded.get(0))
                        : new SpannerObjectName(decoded.get(0), decoded.get(1)));
    }

    /**
     * Splits on the dots outside quotes. Returns {@code null} for an unterminated quote; a quote
     * inside a part is the decoder's to reject.
     */
    private static List<String> split(String objectName, char quote) {
        List<String> parts = new ArrayList<>();
        StringBuilder part = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < objectName.length(); i++) {
            char current = objectName.charAt(i);
            if (quoted && current == '\\' && quote == '`' && i + 1 < objectName.length()) {
                part.append(current).append(objectName.charAt(++i));
                continue;
            }
            if (current == quote) {
                quoted = !quoted;
            } else if (current == '.' && !quoted) {
                parts.add(part.toString());
                part.setLength(0);
                continue;
            }
            part.append(current);
        }
        if (quoted) {
            return null;
        }
        parts.add(part.toString());
        return parts;
    }

    /**
     * Returns the native schema name; the dialect's default schema for an unqualified name.
     *
     * @return the schema
     */
    public String schema() {
        return schema;
    }

    /**
     * Returns the native table name.
     *
     * @return the table
     */
    public String table() {
        return table;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SpannerObjectName)) {
            return false;
        }
        SpannerObjectName that = (SpannerObjectName) other;
        return schema.equals(that.schema) && table.equals(that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, table);
    }

    @Override
    public String toString() {
        return "SpannerObjectName{schema='" + schema + "', table='" + table + "'}";
    }
}
