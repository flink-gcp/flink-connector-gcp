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

package io.github.flink.gcp.connector.bigquery.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * The {@code WITH} options of the {@code bigquery} catalog, created by {@code CREATE CATALOG ...
 * WITH ('type' = 'bigquery', ...)}.
 *
 * <p>The catalog is read-only: it lists one project's datasets as databases and resolves each
 * table's schema from BigQuery. Every key except {@code default-database} is spelled as its {@link
 * BigQueryConnectorOptions} counterpart, and a table the catalog resolves carries the values set
 * here under those keys, so a statement can override them with an {@code OPTIONS} hint. Tuning
 * options stay per statement.
 *
 * <p>The keys are declared here rather than reused from {@link BigQueryConnectorOptions} because
 * the meaning differs: {@code project} is the project whose datasets are listed, not one
 * destination's owner. Like that class, no option has a Flink default and no description restates
 * one (ADR-0139).
 */
@PublicEvolving
public final class BigQueryCatalogOptions {

    /**
     * The Google Cloud project whose datasets the catalog lists as databases and whose tables it
     * resolves. Given as a bare project id, not a resource path.
     */
    public static final ConfigOption<String> PROJECT =
            ConfigOptions.key("project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Google Cloud project whose datasets the catalog lists as"
                                    + " databases and whose tables it resolves. Given as a bare"
                                    + " project id, not a resource path.");

    /** The dataset that a table name without a database resolves against, as a bare dataset id. */
    public static final ConfigOption<String> DEFAULT_DATABASE =
            ConfigOptions.key("default-database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The dataset that a table name without a database resolves against,"
                                    + " as a bare dataset id.");

    /**
     * A path at which the same service-account JSON key is available to the process that plans a
     * statement and to every Job Manager and Task Manager that opens a BigQuery or Cloud Storage
     * client. Tables the catalog resolves carry it.
     */
    public static final ConfigOption<String> SERVICE_ACCOUNT_KEY_FILE =
            ConfigOptions.key("service-account-key-file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A path at which the same service-account JSON key is available to the"
                                    + " process that plans a statement and to every Job Manager and"
                                    + " Task Manager that opens a BigQuery or Cloud Storage client."
                                    + " Tables the catalog resolves carry it.");

    /**
     * A BigQuery emulator's gRPC endpoint as 'host:port'. The catalog does not use it; tables it
     * resolves carry it for Storage Read or Write API traffic. For testing only.
     */
    public static final ConfigOption<String> EMULATOR_ENDPOINT =
            ConfigOptions.key("emulator-endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A BigQuery emulator's gRPC endpoint as 'host:port'. The catalog does"
                                    + " not use it; tables it resolves carry it for Storage Read"
                                    + " or Write API traffic. For testing only.");

    /**
     * A BigQuery emulator's REST endpoint as 'host:port', for the catalog's metadata requests.
     * Tables the catalog resolves carry it. Connects over plaintext without credentials, so it is
     * for testing only.
     */
    public static final ConfigOption<String> EMULATOR_REST_ENDPOINT =
            ConfigOptions.key("emulator-rest-endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A BigQuery emulator's REST endpoint as 'host:port', for the catalog's"
                                    + " metadata requests. Tables the catalog resolves carry it."
                                    + " Connects over plaintext without credentials, so it is for"
                                    + " testing only.");

    /**
     * The project that owns and is billed for the Storage Read sessions of tables the catalog
     * resolves; set it when that is not the catalog's project.
     */
    public static final ConfigOption<String> SCAN_PARENT_PROJECT =
            ConfigOptions.key("scan.parent-project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The project that owns and is billed for the Storage Read sessions of"
                                    + " tables the catalog resolves; set it when that is not the"
                                    + " catalog's project.");

    private BigQueryCatalogOptions() {}
}
