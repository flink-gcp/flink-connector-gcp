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

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * The {@code WITH} options of the {@code spanner} catalog, created by {@code CREATE CATALOG ...
 * WITH ('type' = 'spanner', ...)}.
 *
 * <p>The catalog is read-only: it lists one instance's databases as Flink databases and resolves
 * each table's schema from the database's {@code INFORMATION_SCHEMA}. Every key except {@code
 * default-database} is spelled as its {@link SpannerConnectorOptions} counterpart, and a table the
 * catalog resolves carries the values set here under those keys, so a statement can override them
 * with an {@code OPTIONS} hint. Tuning options stay per statement.
 *
 * <p>The keys are declared here rather than reused from {@link SpannerConnectorOptions} because the
 * scope differs: the catalog reaches every database of the instance, not one table. Like that
 * class, no option has a Flink default and no description restates one (ADR-0139).
 */
@PublicEvolving
public final class SpannerCatalogOptions {

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

    /** The Spanner instance whose databases the catalog lists, given as a bare instance id. */
    public static final ConfigOption<String> INSTANCE =
            ConfigOptions.key("instance")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Spanner instance whose databases the catalog lists, given as a"
                                    + " bare instance id.");

    /** The database that a table name without a database resolves against, as a database id. */
    public static final ConfigOption<String> DEFAULT_DATABASE =
            ConfigOptions.key("default-database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The database that a table name without a database resolves against,"
                                    + " as a database id.");

    /**
     * A path at which the same service-account JSON key is available to the process that plans a
     * statement and to every Job Manager and Task Manager that opens a Spanner client. Tables the
     * catalog resolves carry it.
     */
    public static final ConfigOption<String> SERVICE_ACCOUNT_KEY_FILE =
            ConfigOptions.key("service-account-key-file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A path at which the same service-account JSON key is available to the"
                                    + " process that plans a statement and to every Job Manager and"
                                    + " Task Manager that opens a Spanner client. Tables the"
                                    + " catalog resolves carry it.");

    /**
     * A Spanner emulator's gRPC endpoint as 'host:port', for the catalog's metadata requests and
     * the rows of the tables it resolves, which carry it. Connects over plaintext without
     * credentials, so it is for testing only.
     */
    public static final ConfigOption<String> EMULATOR_ENDPOINT =
            ConfigOptions.key("emulator-endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A Spanner emulator's gRPC endpoint as 'host:port', for the catalog's"
                                    + " metadata requests and the rows of the tables it resolves,"
                                    + " which carry it. Connects over plaintext without"
                                    + " credentials, so it is for testing only.");

    private SpannerCatalogOptions() {}
}
