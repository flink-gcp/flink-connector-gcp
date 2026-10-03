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

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.rpc.ResourceInfo;
import io.github.flink.gcp.connector.spanner.SpannerObjectName;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.protobuf.ProtoUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * An in-memory instance: databases with a dialect, tables with columns, proto types. It applies the
 * two dialects' name comparisons, GoogleSQL's case-insensitive and PostgreSQL's exact, as the
 * production statements do, and records every request so a test can assert that none was made.
 */
class FakeSpannerCatalogClient implements SpannerCatalogClient {

    final Map<String, Dialect> databases = new LinkedHashMap<>();
    final Map<String, List<TableMetadata>> tables = new HashMap<>();
    final Map<String, Map<String, NamedTypeKind>> protoBundleTypes = new HashMap<>();
    final List<String> requests = new ArrayList<>();
    @Nullable RuntimeException failure;
    boolean closed;

    FakeSpannerCatalogClient database(String database, Dialect dialect) {
        databases.put(database, dialect);
        tables.put(database, new ArrayList<>());
        return this;
    }

    FakeSpannerCatalogClient table(
            String database, String schema, String table, ColumnMetadata... columns) {
        tables.get(database)
                .add(
                        new TableMetadata(
                                SpannerObjectName.of(schema, table), Arrays.asList(columns)));
        return this;
    }

    static ColumnMetadata column(String name, String spannerType) {
        return new ColumnMetadata(name, spannerType, true, false, false, false, null);
    }

    static ColumnMetadata key(String name, String spannerType, long position) {
        return new ColumnMetadata(name, spannerType, false, false, false, false, position);
    }

    static SpannerException unavailable() {
        return SpannerExceptionFactory.newSpannerException(ErrorCode.UNAVAILABLE, "boom");
    }

    /**
     * What the client throws for a database that is gone: a {@code NOT_FOUND} carrying the
     * database's {@code ResourceInfo}, which is what makes it a {@code DatabaseNotFoundException}.
     */
    static SpannerException databaseNotFound(String database) {
        Metadata trailers = new Metadata();
        trailers.put(
                ProtoUtils.keyForProto(ResourceInfo.getDefaultInstance()),
                ResourceInfo.newBuilder()
                        .setResourceType(
                                "type.googleapis.com/google.spanner.admin.database.v1.Database")
                        .setResourceName(database)
                        .build());
        return SpannerExceptionFactory.newSpannerException(
                Status.NOT_FOUND
                        .withDescription("Database not found")
                        .asRuntimeException(trailers));
    }

    /** Drops a database: later requests naming it fail as the client fails them. */
    void drop(String database) {
        databases.remove(database);
    }

    @Override
    public List<String> listDatabases() {
        record("listDatabases");
        return new ArrayList<>(databases.keySet());
    }

    @Nullable
    @Override
    public Dialect dialect(String database) {
        record("dialect " + database);
        return databases.get(database);
    }

    @Override
    public List<SpannerObjectName> listTables(String database, Dialect dialect) {
        record("listTables " + database);
        requireDatabase(database);
        List<SpannerObjectName> names = new ArrayList<>();
        for (TableMetadata table : tables.get(database)) {
            names.add(table.name());
        }
        return names;
    }

    @Nullable
    @Override
    public TableMetadata table(String database, Dialect dialect, String schema, String table) {
        record("table " + database + " " + schema + "." + table);
        requireDatabase(database);
        for (TableMetadata candidate : tables.get(database)) {
            if (same(candidate.name().schema(), schema, dialect)
                    && same(candidate.name().table(), table, dialect)) {
                return candidate;
            }
        }
        return null;
    }

    @Override
    public Map<String, NamedTypeKind> protoBundleTypes(String database) {
        record("protoBundleTypes " + database);
        return protoBundleTypes.getOrDefault(database, new HashMap<>());
    }

    @Override
    public void close() {
        closed = true;
    }

    private void requireDatabase(String database) {
        if (!databases.containsKey(database)) {
            throw databaseNotFound(database);
        }
    }

    private void record(String request) {
        requests.add(request);
        if (failure != null) {
            throw failure;
        }
    }

    private static boolean same(String stored, String asked, Dialect dialect) {
        return dialect == Dialect.GOOGLE_STANDARD_SQL
                ? stored.toLowerCase(Locale.ROOT).equals(asked.toLowerCase(Locale.ROOT))
                : stored.equals(asked);
    }
}
