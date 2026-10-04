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
class BigtableCatalogFactoryTest {

    private static Map<String, String> options(String... keyValues) {
        Map<String, String> options = new HashMap<>();
        options.put("type", "bigtable");
        options.put("project", "my-project");
        options.put("instance", "my-instance");
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
                "bt",
                options,
                new Configuration(),
                BigtableCatalogFactoryTest.class.getClassLoader());
    }

    @Test
    void discoversTheCatalogUnderTheConnectorsIdentifier() {
        assertThat(create(options())).isInstanceOf(BigtableCatalog.class);
    }

    @Test
    void createAndUseCatalogNeedNeitherCredentialsNorTheNetwork() {
        // A key file that does not exist: loading it, which opening the client does, would throw.
        // Neither statement reaches a client, so neither reads the file.
        TableEnvironment table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        table.executeSql(
                "CREATE CATALOG bt WITH ('type' = 'bigtable', 'project' = 'my-project',"
                        + " 'instance' = 'my-instance',"
                        + " 'service-account-key-file' = '/nonexistent/key.json')");
        table.executeSql("USE CATALOG bt");

        List<Row> current =
                CollectionUtil.iteratorToList(table.executeSql("SHOW CURRENT DATABASE").collect());
        assertThat(current).containsExactly(Row.of("my-instance"));
        List<Row> databases =
                CollectionUtil.iteratorToList(table.executeSql("SHOW DATABASES").collect());
        assertThat(databases).containsExactly(Row.of("my-instance"));
    }

    /**
     * The catalog's own metadata requests use the key file its tables carry; a statement that
     * reaches Bigtable therefore fails on a key file that does not exist, where one using ADC would
     * not.
     */
    @Test
    void theFirstTableRequestLoadsTheConfiguredKeyFile() {
        TableEnvironment table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        table.executeSql(
                "CREATE CATALOG bt WITH ('type' = 'bigtable', 'project' = 'my-project',"
                        + " 'instance' = 'my-instance',"
                        + " 'service-account-key-file' = '/nonexistent/key.json')");
        table.executeSql("USE CATALOG bt");

        assertThatThrownBy(() -> table.executeSql("SHOW TABLES"))
                .hasStackTraceContaining("Failed to open the Bigtable client of catalog 'bt'")
                .hasStackTraceContaining("service-account key file");
    }

    @Test
    void requiresTheProjectAndInstance() {
        for (String required : new String[] {"project", "instance"}) {
            assertThatThrownBy(() -> create(options(required, null)))
                    .as(required)
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining("Missing required options are:")
                    .hasStackTraceContaining(required);
        }
    }

    @Test
    void rejectsAnUnknownOptionSuchAsATuningKeyOrADefaultDatabase() {
        // Tuning stays per statement (docs/adr/0168); the database is the instance (ADR-0178).
        // FactoryUtil's wrapper lists every option, so the key alone would match the dump.
        for (String key : new String[] {"scan.app-profile-id", "default-database", "table"}) {
            assertThatThrownBy(() -> create(options(key, "x")))
                    .as(key)
                    .isInstanceOf(ValidationException.class)
                    .hasStackTraceContaining("Unsupported options found for 'bigtable'");
        }
    }

    @Test
    void acceptsBothKeyTypesAndRefusesAnyOther() {
        assertThat(create(options("key-type", "string"))).isInstanceOf(BigtableCatalog.class);
        assertThat(create(options("key-type", "bytes"))).isInstanceOf(BigtableCatalog.class);
        assertThatThrownBy(() -> create(options("key-type", "varchar")))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("key-type");
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
    void refusesAKeyFileBesideAnEmulatorEndpoint() {
        assertThatThrownBy(
                        () ->
                                create(
                                        options(
                                                "service-account-key-file", "/key.json",
                                                "emulator-endpoint", "localhost:8086")))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("cannot be combined");
        assertThatThrownBy(() -> create(options("service-account-key-file", " ")))
                .isInstanceOf(ValidationException.class)
                .hasRootCauseMessage("Option 'service-account-key-file' must not be blank.");
    }

    @Test
    void namesTheEndpointKeyWhenTheEndpointIsMalformed() {
        assertThatThrownBy(() -> create(options("emulator-endpoint", "no-port")))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("emulator-endpoint must be host:port");
    }
}
