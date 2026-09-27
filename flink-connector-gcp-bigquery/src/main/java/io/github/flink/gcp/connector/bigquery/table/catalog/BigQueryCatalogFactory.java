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

package io.github.flink.gcp.connector.bigquery.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import io.github.flink.gcp.connector.base.options.ResourceNames;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigquery.sink.tables.BigQueryTableAdmin;
import io.github.flink.gcp.connector.bigquery.table.BigQueryCatalogOptions;
import io.github.flink.gcp.connector.bigquery.table.BigQueryDynamicTableFactory;
import io.github.flink.gcp.connector.bigquery.table.OptionSetters;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Creates the {@code bigquery} catalog from a {@code CREATE CATALOG} statement's options.
 *
 * <p>The identifier is the table connector's, as the JDBC connector uses {@code jdbc} for both;
 * factory discovery tells the two apart by factory type. Every check here reads the options alone:
 * the factory and {@link BigQueryCatalog#open} make no request, so a statement that only creates or
 * selects the catalog needs neither credentials nor the network (docs/adr/0168).
 */
@Internal
public class BigQueryCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier() {
        return BigQueryDynamicTableFactory.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>(
                Arrays.asList(
                        BigQueryCatalogOptions.PROJECT, BigQueryCatalogOptions.DEFAULT_DATABASE));
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>(
                Arrays.asList(
                        BigQueryCatalogOptions.SERVICE_ACCOUNT_KEY_FILE,
                        BigQueryCatalogOptions.EMULATOR_ENDPOINT,
                        BigQueryCatalogOptions.EMULATOR_REST_ENDPOINT,
                        BigQueryCatalogOptions.SCAN_PARENT_PROJECT));
    }

    @Override
    public Catalog createCatalog(Context context) {
        FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();
        ReadableConfig config = helper.getOptions();

        String project = config.get(BigQueryCatalogOptions.PROJECT);
        String defaultDatabase = config.get(BigQueryCatalogOptions.DEFAULT_DATABASE);
        String keyFile =
                config.getOptional(BigQueryCatalogOptions.SERVICE_ACCOUNT_KEY_FILE).orElse(null);
        String emulatorEndpoint =
                config.getOptional(BigQueryCatalogOptions.EMULATOR_ENDPOINT).orElse(null);
        String emulatorRestEndpoint =
                config.getOptional(BigQueryCatalogOptions.EMULATOR_REST_ENDPOINT).orElse(null);
        String parentProject =
                config.getOptional(BigQueryCatalogOptions.SCAN_PARENT_PROJECT).orElse(null);

        BigQueryDynamicTableFactory.checkCredentials(config);
        if ((emulatorEndpoint == null) != (emulatorRestEndpoint == null)) {
            // Either half alone splits the catalog across two services: its metadata requests use
            // the REST endpoint, and the tables it resolves read and write over gRPC. With one
            // endpoint missing, one of the two would reach BigQuery itself on ADC — the tables'
            // rows included, under a schema the emulator answered.
            throw new ValidationException(
                    "Options '"
                            + BigQueryCatalogOptions.EMULATOR_ENDPOINT.key()
                            + "' and '"
                            + BigQueryCatalogOptions.EMULATOR_REST_ENDPOINT.key()
                            + "' go together in a catalog: its metadata requests use the REST"
                            + " endpoint and its tables' rows the gRPC one.");
        }
        // Path components the catalog and its tables compose into resource names (ADR-0127).
        // scan.parent-project takes the noun the table factory checks it under, so its rejection
        // reads the same here as when a table is planned.
        checkComponent(BigQueryCatalogOptions.PROJECT, project, "project");
        checkComponent(BigQueryCatalogOptions.DEFAULT_DATABASE, defaultDatabase, "dataset");
        if (parentProject != null) {
            checkComponent(
                    BigQueryCatalogOptions.SCAN_PARENT_PROJECT, parentProject, "parentProject");
        }
        // Parsed last, after every check that refuses an option outright, as the table factory
        // does: a statement told to remove an option is not helped by an answer about its shape.
        BigQueryDynamicTableFactory.validateEmulatorEndpoints(config);
        EmulatorEndpoint restEndpoint =
                emulatorRestEndpoint == null
                        ? null
                        : EmulatorEndpoint.parse(
                                emulatorRestEndpoint,
                                BigQueryCatalogOptions.EMULATOR_REST_ENDPOINT.key());

        return new BigQueryCatalog(
                context.getName(),
                project,
                defaultDatabase,
                BigQueryCatalog.carriedOptions(
                        keyFile, emulatorEndpoint, emulatorRestEndpoint, parentProject),
                () -> BigQueryTableAdmin.restClient(keyFile, restEndpoint, project));
    }

    private static void checkComponent(ConfigOption<String> option, String value, String noun) {
        OptionSetters.accept(
                option.key(), value, component -> ResourceNames.checkComponent(component, noun));
    }
}
