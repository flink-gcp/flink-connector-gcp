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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import io.github.flink.gcp.connector.base.options.ResourceNames;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.base.table.OptionSetters;
import io.github.flink.gcp.connector.spanner.SpannerClients;
import io.github.flink.gcp.connector.spanner.SpannerCredentials;
import io.github.flink.gcp.connector.spanner.table.SpannerCatalogOptions;
import io.github.flink.gcp.connector.spanner.table.SpannerDynamicTableFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Creates the {@code spanner} catalog from a {@code CREATE CATALOG} statement's options.
 *
 * <p>The identifier is the table connector's, as the JDBC connector uses {@code jdbc} for both;
 * factory discovery tells the two apart by factory type. Every check here reads the options alone:
 * the factory and the catalog's {@code open()} make no request and load no credentials, so a
 * statement that only creates or selects the catalog needs neither (docs/adr/0168).
 */
@Internal
public class SpannerCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier() {
        return SpannerDynamicTableFactory.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>(
                Arrays.asList(
                        SpannerCatalogOptions.PROJECT,
                        SpannerCatalogOptions.INSTANCE,
                        SpannerCatalogOptions.DEFAULT_DATABASE));
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>(
                Arrays.asList(
                        SpannerCatalogOptions.SERVICE_ACCOUNT_KEY_FILE,
                        SpannerCatalogOptions.EMULATOR_ENDPOINT));
    }

    @Override
    public Catalog createCatalog(Context context) {
        FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();
        ReadableConfig config = helper.getOptions();

        String project = config.get(SpannerCatalogOptions.PROJECT);
        String instance = config.get(SpannerCatalogOptions.INSTANCE);
        String defaultDatabase = config.get(SpannerCatalogOptions.DEFAULT_DATABASE);
        String keyFile =
                config.getOptional(SpannerCatalogOptions.SERVICE_ACCOUNT_KEY_FILE).orElse(null);
        String emulatorEndpoint =
                config.getOptional(SpannerCatalogOptions.EMULATOR_ENDPOINT).orElse(null);

        // The catalog's keys are the connector's, so the table factory's checks read them as is.
        SpannerDynamicTableFactory.validateCredentialsMode(config);
        // Path components the catalog and its tables compose into resource names (ADR-0127).
        checkComponent(SpannerCatalogOptions.PROJECT, project, "project");
        checkComponent(SpannerCatalogOptions.INSTANCE, instance, "instance");
        checkComponent(SpannerCatalogOptions.DEFAULT_DATABASE, defaultDatabase, "database");
        // A default database outside the id grammar names no database, and the catalog answers
        // such a name without asking Spanner; refusing it here names the option rather than
        // leaving every unqualified table "not found" (ADR-0127, a value the catalog parses).
        if (!SpannerCatalog.DATABASE_ID.matcher(defaultDatabase).matches()) {
            throw new ValidationException(
                    "Option '"
                            + SpannerCatalogOptions.DEFAULT_DATABASE.key()
                            + "' must be a Spanner database id: 2 to 30 lower-case letters,"
                            + " digits, underscores or hyphens, starting with a letter and ending"
                            + " with a letter or digit, but was '"
                            + defaultDatabase
                            + "'.");
        }
        // Parsed last, after every check that refuses an option outright, as the table factory
        // does: a statement told to remove an option is not helped by an answer about its shape.
        EmulatorEndpoint endpoint =
                emulatorEndpoint == null
                        ? null
                        : EmulatorEndpoint.parse(
                                emulatorEndpoint, SpannerCatalogOptions.EMULATOR_ENDPOINT.key());

        return new SpannerCatalog(
                context.getName(),
                project,
                instance,
                defaultDatabase,
                SpannerCatalog.carriedOptions(keyFile, emulatorEndpoint),
                () ->
                        new SpannerServiceCatalogClient(
                                SpannerClients.open(
                                        "Spanner instance " + project + "/" + instance,
                                        SpannerClients.settings(
                                                project,
                                                endpoint,
                                                SpannerCredentials.load(keyFile))),
                                project,
                                instance));
    }

    private static void checkComponent(ConfigOption<String> option, String value, String noun) {
        OptionSetters.accept(
                option.key(), value, component -> ResourceNames.checkComponent(component, noun));
    }
}
