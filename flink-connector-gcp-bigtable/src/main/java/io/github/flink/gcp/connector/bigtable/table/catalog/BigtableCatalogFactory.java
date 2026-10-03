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

package io.github.flink.gcp.connector.bigtable.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import io.github.flink.gcp.connector.base.options.ResourceNames;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.BigtableCredentials;
import io.github.flink.gcp.connector.bigtable.table.BigtableCatalogOptions;
import io.github.flink.gcp.connector.bigtable.table.BigtableDynamicTableFactory;
import io.github.flink.gcp.connector.bigtable.table.CatalogKeyType;
import io.github.flink.gcp.connector.bigtable.table.OptionSetters;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Creates the {@code bigtable} catalog from a {@code CREATE CATALOG} statement's options.
 *
 * <p>The identifier is the table connector's, as the JDBC connector uses {@code jdbc} for both;
 * factory discovery tells the two apart by factory type. Every check here reads the options alone:
 * the factory and the catalog's {@code open()} make no request and load no credentials, so a
 * statement that only creates or selects the catalog needs neither (docs/adr/0168).
 */
@Internal
public class BigtableCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier() {
        return BigtableDynamicTableFactory.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>(
                Arrays.asList(BigtableCatalogOptions.PROJECT, BigtableCatalogOptions.INSTANCE));
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>(
                Arrays.asList(
                        BigtableCatalogOptions.KEY_TYPE,
                        BigtableCatalogOptions.SERVICE_ACCOUNT_KEY_FILE,
                        BigtableCatalogOptions.EMULATOR_ENDPOINT));
    }

    @Override
    public Catalog createCatalog(Context context) {
        FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();
        ReadableConfig config = helper.getOptions();

        String project = config.get(BigtableCatalogOptions.PROJECT);
        String instance = config.get(BigtableCatalogOptions.INSTANCE);
        CatalogKeyType keyType = config.get(BigtableCatalogOptions.KEY_TYPE);
        String keyFile =
                config.getOptional(BigtableCatalogOptions.SERVICE_ACCOUNT_KEY_FILE).orElse(null);
        String emulatorEndpoint =
                config.getOptional(BigtableCatalogOptions.EMULATOR_ENDPOINT).orElse(null);

        // The catalog's keys are the connector's, so the table factory's check reads them as is.
        BigtableDynamicTableFactory.validateCredentialsMode(config);
        // Path components the catalog and its tables compose into resource names (ADR-0127).
        checkComponent(BigtableCatalogOptions.PROJECT, project, "project");
        checkComponent(BigtableCatalogOptions.INSTANCE, instance, "instance");
        // Parsed last, after every check that refuses an option outright, as the table factory
        // does: a statement told to remove an option is not helped by an answer about its shape.
        EmulatorEndpoint endpoint =
                emulatorEndpoint == null
                        ? null
                        : EmulatorEndpoint.parse(
                                emulatorEndpoint, BigtableCatalogOptions.EMULATOR_ENDPOINT.key());

        return new BigtableCatalog(
                context.getName(),
                project,
                instance,
                keyType,
                BigtableCatalog.carriedOptions(keyFile, emulatorEndpoint),
                () ->
                        BigtableServiceCatalogClient.open(
                                project,
                                instance,
                                endpoint,
                                BigtableCredentials.loadTableAdmin(keyFile)));
    }

    private static void checkComponent(ConfigOption<String> option, String value, String noun) {
        OptionSetters.accept(
                option.key(), value, component -> ResourceNames.checkComponent(component, noun));
    }
}
