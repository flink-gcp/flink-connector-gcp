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

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.DatabaseNotFoundException;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.flink.gcp.connector.spanner.SpannerObjectName;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * {@link SpannerCatalogClient} over one {@link Spanner} handle: the instance's database admin
 * client for databases, dialects and proto descriptors, and each database's data client for {@code
 * INFORMATION_SCHEMA}. The handle carries only the project; each call names its database.
 */
@Internal
final class SpannerServiceCatalogClient implements SpannerCatalogClient {

    private final Spanner spanner;
    private final String project;
    private final String instance;

    SpannerServiceCatalogClient(Spanner spanner, String project, String instance) {
        this.spanner = spanner;
        this.project = project;
        this.instance = instance;
    }

    @Override
    public List<String> listDatabases() {
        List<String> databases = new ArrayList<>();
        for (Database database : admin().listDatabases(instance).iterateAll()) {
            databases.add(database.getId().getDatabase());
        }
        return databases;
    }

    @Nullable
    @Override
    public Dialect dialect(String database) {
        Database found;
        try {
            found = admin().getDatabase(instance, database);
        } catch (DatabaseNotFoundException e) {
            return null;
        }
        // The client maps an unspecified dialect to GOOGLE_STANDARD_SQL, the service's default.
        return found.getDialect();
    }

    @Override
    public List<SpannerObjectName> listTables(String database, Dialect dialect) {
        return query(
                database,
                SpannerInformationSchema.listTables(dialect),
                rows -> {
                    List<SpannerObjectName> tables = new ArrayList<>();
                    while (rows.next()) {
                        tables.add(SpannerObjectName.of(rows.getString(0), rows.getString(1)));
                    }
                    return tables;
                });
    }

    @Nullable
    @Override
    public TableMetadata table(String database, Dialect dialect, String schema, String table) {
        return query(
                database,
                SpannerInformationSchema.columnsOf(dialect, schema, table),
                rows -> {
                    SpannerObjectName name = null;
                    List<ColumnMetadata> columns = new ArrayList<>();
                    while (rows.next()) {
                        if (name == null) {
                            name = SpannerObjectName.of(rows.getString(0), rows.getString(1));
                        }
                        columns.add(
                                new ColumnMetadata(
                                        rows.getString(2),
                                        rows.getString(3),
                                        "YES".equals(rows.getString(4)),
                                        "ALWAYS".equals(rows.getString(5)),
                                        !rows.isNull(8) && "YES".equals(rows.getString(8)),
                                        hidden(rows, dialect),
                                        rows.isNull(7) ? null : rows.getLong(7)));
                    }
                    return name == null ? null : new TableMetadata(name, columns);
                });
    }

    /** GoogleSQL reports {@code IS_HIDDEN} as a BOOL, PostgreSQL as {@code YES}/{@code NO}. */
    private static boolean hidden(ResultSet rows, Dialect dialect) {
        if (rows.isNull(6)) {
            return false;
        }
        return dialect == Dialect.POSTGRESQL ? "YES".equals(rows.getString(6)) : rows.getBoolean(6);
    }

    @Override
    public Map<String, NamedTypeKind> protoBundleTypes(String database) {
        ByteString descriptors =
                admin().getDatabaseDdlResponse(instance, database).getProtoDescriptors();
        if (descriptors.isEmpty()) {
            return new HashMap<>();
        }
        try {
            return kindsOf(FileDescriptorSet.parseFrom(descriptors));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(
                    "Spanner returned proto descriptors for database '"
                            + database
                            + "' that do not parse as a FileDescriptorSet.",
                    e);
        }
    }

    /**
     * The kind of every message and enum a descriptor set declares, by the fully qualified name a
     * column's {@code SPANNER_TYPE} uses: the package, then each enclosing message, then the name.
     */
    static Map<String, NamedTypeKind> kindsOf(FileDescriptorSet files) {
        Map<String, NamedTypeKind> kinds = new HashMap<>();
        for (FileDescriptorProto file : files.getFileList()) {
            String prefix = file.getPackage().isEmpty() ? "" : file.getPackage() + ".";
            for (DescriptorProto message : file.getMessageTypeList()) {
                collect(prefix, message, kinds);
            }
            for (EnumDescriptorProto enumType : file.getEnumTypeList()) {
                kinds.put(prefix + enumType.getName(), NamedTypeKind.ENUM);
            }
        }
        return kinds;
    }

    private static void collect(
            String prefix, DescriptorProto message, Map<String, NamedTypeKind> kinds) {
        String name = prefix + message.getName();
        kinds.put(name, NamedTypeKind.PROTO);
        for (DescriptorProto nested : message.getNestedTypeList()) {
            collect(name + ".", nested, kinds);
        }
        for (EnumDescriptorProto enumType : message.getEnumTypeList()) {
            kinds.put(name + "." + enumType.getName(), NamedTypeKind.ENUM);
        }
    }

    @Override
    public void close() {
        spanner.close();
    }

    private DatabaseAdminClient admin() {
        return spanner.getDatabaseAdminClient();
    }

    private DatabaseId id(String database) {
        return DatabaseId.of(project, instance, database);
    }

    private <T> T query(String database, Statement statement, Function<ResultSet, T> reader) {
        try (ResultSet rows =
                spanner.getDatabaseClient(id(database)).singleUse().executeQuery(statement)) {
            return reader.apply(rows);
        }
    }
}
