/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigquery.source;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializer;
import org.apache.avro.generic.GenericRecord;

import java.util.function.UnaryOperator;

/**
 * Builds source configurations for tests in the source's subpackages, which cannot reach the
 * configuration's package-private constructor.
 */
public final class TestSources {

    public static final TableDestination TABLE = TableDestination.of("p", "d", "t");

    private TestSources() {}

    /** Returns the configuration of a source built with the defaults. */
    public static BigQuerySourceConfig<GenericRecord> config() {
        return config(UnaryOperator.identity());
    }

    /**
     * Returns the configuration of a source built with the given knobs applied.
     *
     * @param customizer applies the knobs a test needs
     * @return the configuration
     */
    public static BigQuerySourceConfig<GenericRecord> config(
            UnaryOperator<BigQuerySourceBuilder<GenericRecord>> customizer) {
        BigQuerySourceBuilder<GenericRecord> builder =
                BigQuerySource.<GenericRecord>builder()
                        .table(TABLE)
                        .deserializer(BigQueryRowDeserializer.genericRecord(TestRows.SCHEMA_JSON))
                        // The builder creates this source's real clients, which would demand
                        // application-default credentials on a machine that has them and fail in CI
                        // on one that does not. The endpoint is never connected to.
                        .emulatorEndpoint("localhost:1");
        return ((BigQueryStorageReadSource<GenericRecord>) customizer.apply(builder).build())
                .getConfig();
    }
}
