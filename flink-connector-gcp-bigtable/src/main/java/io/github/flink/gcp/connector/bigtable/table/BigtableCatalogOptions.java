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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * The {@code WITH} options of the {@code bigtable} catalog, created by {@code CREATE CATALOG ...
 * WITH ('type' = 'bigtable', ...)}.
 *
 * <p>The catalog is read-only: it lists one instance's tables in one Flink database named after the
 * instance, and resolves each table as a row key column plus one {@code MAP} column per column
 * family. Every key except {@code key-type} is spelled as its {@link BigtableConnectorOptions}
 * counterpart, and a table the catalog resolves carries the values set here under those keys, so a
 * statement can override them with an {@code OPTIONS} hint. Tuning options stay per statement.
 *
 * <p>The keys are declared here rather than reused from {@link BigtableConnectorOptions} because
 * the scope differs: the catalog reaches every table of the instance, not one table.
 */
@PublicEvolving
public final class BigtableCatalogOptions {

    /**
     * The Google Cloud project that owns the instance, given as a bare project id, not a resource
     * path.
     */
    public static final ConfigOption<String> PROJECT =
            ConfigOptions.key("project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Google Cloud project that owns the instance, given as a bare"
                                    + " project id, not a resource path.");

    /**
     * The Bigtable instance whose tables the catalog lists, given as a bare instance id. It is also
     * the name of the catalog's one database.
     */
    public static final ConfigOption<String> INSTANCE =
            ConfigOptions.key("instance")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Bigtable instance whose tables the catalog lists, given as a bare"
                                    + " instance id. It is also the name of the catalog's one"
                                    + " database.");

    /**
     * The Flink type of each table's '_key' row key column and of the keys of its column family
     * maps. 'bytes' keeps the stored bytes; 'string' reads them as UTF-8 without validating them,
     * so a row key or a qualifier compares with a string literal.
     */
    public static final ConfigOption<CatalogKeyType> KEY_TYPE =
            ConfigOptions.key("key-type")
                    .enumType(CatalogKeyType.class)
                    .defaultValue(CatalogKeyType.BYTES)
                    .withDescription(
                            "The Flink type of each table's '_key' row key column and of the keys"
                                    + " of its column family maps. 'bytes' keeps the stored bytes;"
                                    + " 'string' reads them as UTF-8 without validating them, so a"
                                    + " row key or a qualifier compares with a string literal.");

    /**
     * A path at which the same service-account JSON key is available to the process that plans a
     * statement and to every Job Manager and Task Manager that opens a Bigtable client. Tables the
     * catalog resolves carry it.
     */
    public static final ConfigOption<String> SERVICE_ACCOUNT_KEY_FILE =
            ConfigOptions.key("service-account-key-file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A path at which the same service-account JSON key is available to the"
                                    + " process that plans a statement and to every Job Manager and"
                                    + " Task Manager that opens a Bigtable client. Tables the"
                                    + " catalog resolves carry it.");

    /**
     * A Bigtable emulator's endpoint as 'host:port', for the catalog's metadata requests and the
     * rows of the tables it resolves, which carry it. Connects over plaintext without credentials,
     * so it is for testing only.
     */
    public static final ConfigOption<String> EMULATOR_ENDPOINT =
            ConfigOptions.key("emulator-endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A Bigtable emulator's endpoint as 'host:port', for the catalog's"
                                    + " metadata requests and the rows of the tables it resolves,"
                                    + " which carry it. Connects over plaintext without"
                                    + " credentials, so it is for testing only.");

    private BigtableCatalogOptions() {}
}
