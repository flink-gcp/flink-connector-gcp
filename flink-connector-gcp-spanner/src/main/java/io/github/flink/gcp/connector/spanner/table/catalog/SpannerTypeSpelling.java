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

package io.github.flink.gcp.connector.spanner.table.catalog;

import org.apache.flink.annotation.Internal;

import com.google.cloud.spanner.Dialect;

import javax.annotation.Nullable;

import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A column's type as {@code INFORMATION_SCHEMA.COLUMNS.SPANNER_TYPE} spells it, parsed into the
 * kind the connector's type mapping is written in.
 *
 * <p>The spellings are the ones Spanner reports, which are not always the ones a DDL statement
 * used: a PostgreSQL {@code text} column reports {@code character varying}. Spanner reports a PROTO
 * or ENUM column as {@code PROTO<pkg.Message>} or {@code ENUM<pkg.Enum>}, while the emulator
 * reports only the fully qualified name in backticks, with nothing marking which of the two it is.
 * That is why a {@link Kind#NAMED} type carries only its name; the catalog classifies it from the
 * database's proto descriptors. A type the connector has no mapping for parses as {@link
 * Kind#UNSUPPORTED} rather than failing, so the refusal can name the column.
 */
@Internal
final class SpannerTypeSpelling {

    /** The scalar kinds of the connector's type mapping, plus the two that are not mapped. */
    enum Kind {
        BOOL,
        INT64,
        FLOAT32,
        FLOAT64,
        NUMERIC,
        STRING,
        BYTES,
        DATE,
        TIMESTAMP,
        JSON,
        UUID,
        /** A PROTO message, by its fully qualified name. */
        PROTO,
        /** An ENUM type, by its fully qualified name. */
        ENUM,
        /**
         * A PROTO message or ENUM type named without its kind, by its fully qualified name; the
         * emulator spells both this way, so the catalog classifies them from the proto bundle.
         */
        NAMED,
        UNSUPPORTED
    }

    private static final Pattern GOOGLE_SQL_ARRAY =
            Pattern.compile("ARRAY<(.+)>(?:\\s*\\(\\s*vector_length\\s*=>\\s*\\d+\\s*\\))?");
    private static final Pattern GOOGLE_SQL_SIZED =
            Pattern.compile("(STRING|BYTES)\\((MAX|\\d+)\\)");
    private static final Pattern QUOTED_NAME = Pattern.compile("`([A-Za-z_][A-Za-z0-9_.]*)`");
    private static final Pattern KINDED_NAME =
            Pattern.compile("(PROTO|ENUM)<`?([A-Za-z_][A-Za-z0-9_.]*)`?>");
    private static final Pattern POSTGRESQL_VECTOR =
            Pattern.compile("(.+\\[\\])\\s+vector\\s+length\\s+\\d+");
    private static final Pattern POSTGRESQL_VARCHAR =
            Pattern.compile("(?:character varying|varchar)(?:\\(\\d+\\))?");

    private final Kind kind;
    private final boolean array;
    @Nullable private final String typeName;

    private SpannerTypeSpelling(Kind kind, boolean array, @Nullable String typeName) {
        this.kind = kind;
        this.array = array;
        this.typeName = typeName;
    }

    /**
     * Parses a {@code SPANNER_TYPE} value.
     *
     * @param spannerType the value as {@code INFORMATION_SCHEMA} reports it
     * @param dialect the database's dialect
     * @return the type; {@link Kind#UNSUPPORTED} for a spelling the mapping does not cover
     */
    static SpannerTypeSpelling parse(String spannerType, Dialect dialect) {
        String trimmed = spannerType.trim();
        // A PostgreSQL vector column has been seen reported in the GoogleSQL spelling.
        if (dialect == Dialect.GOOGLE_STANDARD_SQL || trimmed.startsWith("ARRAY<")) {
            Matcher array = GOOGLE_SQL_ARRAY.matcher(trimmed);
            if (array.matches()) {
                SpannerTypeSpelling element = googleSqlScalar(array.group(1).trim());
                return new SpannerTypeSpelling(element.kind, true, element.typeName);
            }
            if (dialect == Dialect.GOOGLE_STANDARD_SQL) {
                return googleSqlScalar(trimmed);
            }
        }
        String lower = trimmed.toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");
        Matcher vector = POSTGRESQL_VECTOR.matcher(lower);
        if (vector.matches()) {
            lower = vector.group(1);
        }
        boolean array = lower.endsWith("[]");
        Kind kind = postgreSqlScalar(array ? lower.substring(0, lower.length() - 2).trim() : lower);
        return new SpannerTypeSpelling(kind, array, null);
    }

    private static SpannerTypeSpelling googleSqlScalar(String type) {
        Matcher kinded = KINDED_NAME.matcher(type);
        if (kinded.matches()) {
            return new SpannerTypeSpelling(
                    kinded.group(1).equals("PROTO") ? Kind.PROTO : Kind.ENUM,
                    false,
                    kinded.group(2));
        }
        Matcher quoted = QUOTED_NAME.matcher(type);
        if (quoted.matches()) {
            return new SpannerTypeSpelling(Kind.NAMED, false, quoted.group(1));
        }
        Matcher sized = GOOGLE_SQL_SIZED.matcher(type);
        if (sized.matches()) {
            return scalar(sized.group(1).equals("STRING") ? Kind.STRING : Kind.BYTES);
        }
        switch (type) {
            case "BOOL":
                return scalar(Kind.BOOL);
            case "INT64":
                return scalar(Kind.INT64);
            case "FLOAT32":
                return scalar(Kind.FLOAT32);
            case "FLOAT64":
                return scalar(Kind.FLOAT64);
            case "NUMERIC":
                return scalar(Kind.NUMERIC);
            case "DATE":
                return scalar(Kind.DATE);
            case "TIMESTAMP":
                return scalar(Kind.TIMESTAMP);
            case "JSON":
                return scalar(Kind.JSON);
            case "UUID":
                return scalar(Kind.UUID);
            default:
                return scalar(Kind.UNSUPPORTED);
        }
    }

    private static Kind postgreSqlScalar(String type) {
        if (POSTGRESQL_VARCHAR.matcher(type).matches() || type.equals("text")) {
            return Kind.STRING;
        }
        switch (type) {
            case "boolean":
            case "bool":
                return Kind.BOOL;
            case "bigint":
            case "int8":
                return Kind.INT64;
            case "real":
            case "float4":
                return Kind.FLOAT32;
            case "double precision":
            case "float8":
                return Kind.FLOAT64;
            case "numeric":
            case "decimal":
                return Kind.NUMERIC;
            case "bytea":
                return Kind.BYTES;
            case "date":
                return Kind.DATE;
            case "timestamp with time zone":
            case "timestamptz":
            case "spanner.commit_timestamp":
                return Kind.TIMESTAMP;
            case "jsonb":
                return Kind.JSON;
            case "uuid":
                return Kind.UUID;
            default:
                return Kind.UNSUPPORTED;
        }
    }

    private static SpannerTypeSpelling scalar(Kind kind) {
        return new SpannerTypeSpelling(kind, false, null);
    }

    Kind kind() {
        return kind;
    }

    boolean isArray() {
        return array;
    }

    /**
     * The fully qualified name of a {@link Kind#PROTO}, {@link Kind#ENUM} or {@link Kind#NAMED}
     * type; {@code null} for any other kind.
     */
    @Nullable
    String typeName() {
        return typeName;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SpannerTypeSpelling)) {
            return false;
        }
        SpannerTypeSpelling that = (SpannerTypeSpelling) other;
        return kind == that.kind && array == that.array && Objects.equals(typeName, that.typeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, array, typeName);
    }

    @Override
    public String toString() {
        String scalar = typeName != null ? kind.name() + "<" + typeName + ">" : kind.name();
        return array ? "ARRAY<" + scalar + ">" : scalar;
    }
}
