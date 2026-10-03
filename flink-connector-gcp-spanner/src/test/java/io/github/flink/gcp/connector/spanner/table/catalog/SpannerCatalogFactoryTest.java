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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The {@code CREATE CATALOG} surface: discovery, validation, and no request before a lookup. */
class SpannerCatalogFactoryTest {

    private static Map<String, String> options(String... keyValues) {
        Map<String, String> options = new HashMap<>();
        options.put("type", "spanner");
        options.put("project", "my-project");
        options.put("instance", "my-instance");
        options.put("default-database", "orders-db");
        for (int i = 0; i < keyValues.length; i += 2) {
            if (keyValues[i + 1] == null) {
                options.remove(keyValues[i]);
            } else {
                options.put(keyValues[i], keyValues[i + 1]);
            }
        }
        return options;
    }

    private static Catalog create(Map<String, String> options) {
        return FactoryUtil.createCatalog(
                "sp",
                options,
                new Configuration(),
                SpannerCatalogFactoryTest.class.getClassLoader());
    }

    @Test
    void discoversTheCatalogUnderTheConnectorsIdentifier() {
        assertThat(create(options())).isInstanceOf(SpannerCatalog.class);
    }

    @Test
    void createAndUseCatalogNeedNeitherCredentialsNorTheNetwork() {
        // A key file that does not exist: loading it, which opening the client does, would throw.
        // Neither statement reaches a client, so neither reads the file.
        TableEnvironment table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        table.executeSql(
                "CREATE CATALOG sp WITH ('type' = 'spanner', 'project' = 'my-project',"
                        + " 'instance' = 'my-instance', 'default-database' = 'orders-db',"
                        + " 'service-account-key-file' = '/nonexistent/key.json')");
        table.executeSql("USE CATALOG sp");

        List<Row> current =
                CollectionUtil.iteratorToList(table.executeSql("SHOW CURRENT DATABASE").collect());
        assertThat(current).containsExactly(Row.of("orders-db"));
    }

    /**
     * The catalog's own metadata requests use the key file its tables carry; a statement that
     * reaches Spanner therefore fails on a key file that does not exist, where one using ADC would
     * not.
     */
    @Test
    void theFirstMetadataCallLoadsTheConfiguredKeyFile() {
        TableEnvironment table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        table.executeSql(
                "CREATE CATALOG sp WITH ('type' = 'spanner', 'project' = 'my-project',"
                        + " 'instance' = 'my-instance', 'default-database' = 'orders-db',"
                        + " 'service-account-key-file' = '/nonexistent/key.json')");
        table.executeSql("USE CATALOG sp");

        assertThatThrownBy(() -> table.executeSql("SHOW TABLES"))
                .hasStackTraceContaining("Failed to open the Spanner client of catalog 'sp'")
                .hasStackTraceContaining("service-account key file");
    }

    @Test
    void requiresTheProjectInstanceAndDefaultDatabase() {
        for (String required : new String[] {"project", "instance", "default-database"}) {
            assertThatThrownBy(() -> create(options(required, null)))
                    .as(required)
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining("Missing required options are:")
                    .hasStackTraceContaining(required);
        }
    }

    @Test
    void rejectsAnUnknownOptionSuchAsATuningKey() {
        // Tuning stays per statement; a catalog-level default is a non-goal (docs/adr/0168).
        // FactoryUtil's wrapper lists every option, so the key alone would match the dump.
        assertThatThrownBy(() -> create(options("scan.data-boost-enabled", "true")))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("Unsupported options found for 'spanner'");
    }

    @Test
    void namesTheOptionKeyWhenAPathComponentIsRejected() {
        assertThatThrownBy(() -> create(options("instance", "a/b")))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("Option 'instance' is invalid")
                .hasStackTraceContaining("instance must not contain '/'");
        assertThatThrownBy(() -> create(options("project", " p")))
                .hasStackTraceContaining("Option 'project' is invalid")
                .hasStackTraceContaining("project must not have leading or trailing whitespace");
    }

    @Test
    void refusesADefaultDatabaseOutsideTheDatabaseIdGrammar() {
        for (String id : new String[] {"Orders", "a", "orders_", "1orders"}) {
            assertThatThrownBy(() -> create(options("default-database", id)))
                    .as(id)
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining(
                            "Option 'default-database' must be a Spanner database id");
        }
    }

    @Test
    void refusesAKeyFileBesideAnEmulatorEndpoint() {
        assertThatThrownBy(
                        () ->
                                create(
                                        options(
                                                "service-account-key-file", "/key.json",
                                                "emulator-endpoint", "localhost:9010")))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining(
                        "service-account-key-file cannot be combined with emulator-endpoint");
        assertThatThrownBy(() -> create(options("service-account-key-file", " ")))
                .hasStackTraceContaining("service-account-key-file must not be blank");
    }

    @Test
    void namesTheEndpointKeyWhenTheEndpointIsMalformed() {
        assertThatThrownBy(() -> create(options("emulator-endpoint", "no-port")))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("emulator-endpoint must be host:port");
    }
}
