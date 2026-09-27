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
class BigQueryCatalogFactoryTest {

    private static Map<String, String> options(String... keyValues) {
        Map<String, String> options = new HashMap<>();
        options.put("type", "bigquery");
        options.put("project", "my-project");
        options.put("default-database", "analytics");
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
                "bq",
                options,
                new Configuration(),
                BigQueryCatalogFactoryTest.class.getClassLoader());
    }

    @Test
    void discoversTheCatalogUnderTheConnectorsIdentifier() {
        assertThat(create(options())).isInstanceOf(BigQueryCatalog.class);
    }

    @Test
    void createAndUseCatalogNeedNeitherCredentialsNorTheNetwork() {
        // A key file that does not exist: loading it, which building a client does, would throw.
        // Neither statement reaches a client, so neither reads the file.
        TableEnvironment table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        table.executeSql(
                "CREATE CATALOG bq WITH ('type' = 'bigquery', 'project' = 'my-project',"
                        + " 'default-database' = 'analytics',"
                        + " 'service-account-key-file' = '/nonexistent/key.json')");
        table.executeSql("USE CATALOG bq");

        List<Row> current =
                CollectionUtil.iteratorToList(table.executeSql("SHOW CURRENT DATABASE").collect());
        assertThat(current).containsExactly(Row.of("analytics"));
    }

    @Test
    void requiresTheProjectAndTheDefaultDatabase() {
        assertThatThrownBy(() -> create(options("project", null)))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("project");
        assertThatThrownBy(() -> create(options("default-database", null)))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("default-database");
    }

    @Test
    void rejectsAnUnknownOptionSuchAsATuningKey() {
        // Tuning stays per statement; a catalog-level default is a non-goal (docs/adr/0168).
        // FactoryUtil's wrapper lists every option, so the key alone would match the dump.
        assertThatThrownBy(() -> create(options("sink.write-method", "file-loads")))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("Unsupported options found for 'bigquery'");
    }

    @Test
    void namesTheOptionKeyWhenAPathComponentIsRejected() {
        assertThatThrownBy(() -> create(options("default-database", "a/b")))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("Option 'default-database' is invalid")
                .hasStackTraceContaining("dataset must not contain '/'");
        assertThatThrownBy(() -> create(options("project", " p")))
                .hasStackTraceContaining("Option 'project' is invalid")
                .hasStackTraceContaining("project must not have leading or trailing whitespace");
        // The table factory's noun, so the sentence reads the same when a table is planned.
        assertThatThrownBy(() -> create(options("scan.parent-project", "a/b")))
                .hasStackTraceContaining("Option 'scan.parent-project' is invalid")
                .hasStackTraceContaining("parentProject must not contain '/'");
    }

    @Test
    void refusesAKeyFileBesideAnEmulatorEndpoint() {
        assertThatThrownBy(
                        () ->
                                create(
                                        options(
                                                "service-account-key-file", "/key.json",
                                                "emulator-rest-endpoint", "localhost:9050")))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("credential-free");
        assertThatThrownBy(() -> create(options("service-account-key-file", " ")))
                .hasStackTraceContaining("must not be blank");
    }

    @Test
    void namesTheEndpointKeyWhenAnEndpointIsMalformed() {
        // The parser's own sentence, which names the key it was given; the key alone would match
        // FactoryUtil's dump of the options.
        assertThatThrownBy(
                        () ->
                                create(
                                        options(
                                                "emulator-endpoint", "localhost:9060",
                                                "emulator-rest-endpoint", "no-port")))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("emulator-rest-endpoint must be host:port");
        assertThatThrownBy(
                        () ->
                                create(
                                        options(
                                                "emulator-endpoint", "no-port",
                                                "emulator-rest-endpoint", "localhost:9050")))
                .hasStackTraceContaining("emulator-endpoint must be host:port");
    }

    @Test
    void refusesEitherEmulatorEndpointWithoutTheOther() {
        // Either alone sends one half of the catalog to the service: the metadata requests
        // without the REST endpoint, the tables' rows without the gRPC one.
        assertThatThrownBy(() -> create(options("emulator-endpoint", "localhost:9060")))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining(
                        "Options 'emulator-endpoint' and 'emulator-rest-endpoint' go together");
        assertThatThrownBy(() -> create(options("emulator-rest-endpoint", "localhost:9050")))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining(
                        "Options 'emulator-endpoint' and 'emulator-rest-endpoint' go together");
    }
}
