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

package io.github.flink.gcp.connector.bigtable.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import com.google.cloud.bigtable.data.v2.models.Row;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.readrows.BigtableReadRowsSource;
import io.github.flink.gcp.connector.bigtable.source.readrows.enumerator.RowKeySamplerFactory;
import io.github.flink.gcp.connector.bigtable.source.readrows.reader.RowStreamOpener;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableChangeStreamMutationDeserializationSchema;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableRowDeserializationSchema;

import java.util.function.UnaryOperator;

/**
 * Builds source configurations for tests in the sources' subpackages, which cannot reach the
 * builders' test-only setters or the Change Streams source's package-private configuration
 * accessor.
 */
public final class TestSources {

    /** The table every fixture reads. */
    public static final TableDestination TABLE = TableDestination.of("p", "i", "orders");

    private TestSources() {}

    /** Returns the configuration of a source built with the defaults. */
    public static BigtableSourceConfig<String> config() {
        return config(UnaryOperator.identity());
    }

    /**
     * Returns the configuration of a source built with the given knobs applied.
     *
     * @param customizer applies the knobs a test needs
     * @return the configuration
     */
    public static BigtableSourceConfig<String> config(
            UnaryOperator<BigtableSourceBuilder<String>> customizer) {
        return source(customizer).getConfig();
    }

    /**
     * Returns a source built with the given knobs applied, as its implementation type.
     *
     * @param customizer applies the knobs a test needs
     * @return the source
     */
    public static BigtableReadRowsSource<String> source(
            UnaryOperator<BigtableSourceBuilder<String>> customizer) {
        BigtableSourceBuilder<String> builder =
                BigtableSource.<String>builder()
                        .table(TABLE)
                        .deserializer(new RowKeyDeserializer())
                        // The builder creates this source's real clients, which would demand
                        // application-default credentials on a machine that has them and fail in CI
                        // on one that does not. The endpoint is never connected to.
                        .emulatorEndpoint("localhost:1");
        return (BigtableReadRowsSource<String>) customizer.apply(builder).build();
    }

    /**
     * Returns the configuration of a Change Streams source built with the given knobs applied on
     * top of the fixture's table, a single-cluster application profile and the mutation
     * deserializer. No client is created and nothing connects: the builder only assembles the
     * configuration this returns.
     *
     * @param customizer applies the knobs a test needs
     * @return the configuration
     */
    public static BigtableChangeStreamSourceConfig<BigtableChangeStreamMutation> changeStreamConfig(
            UnaryOperator<BigtableChangeStreamSourceBuilder<BigtableChangeStreamMutation>>
                    customizer) {
        BigtableChangeStreamSourceBuilder<BigtableChangeStreamMutation> builder =
                BigtableChangeStreamSource.<BigtableChangeStreamMutation>builder()
                        .table(TABLE)
                        .appProfileId("single-cluster")
                        .deserializer(new BigtableChangeStreamMutationDeserializationSchema());
        return customizer.apply(builder).build().getConfig();
    }

    /**
     * Applies a sampler-factory seam to a builder, reaching the package-private setter for a
     * subpackage.
     */
    public static BigtableSourceBuilder<String> withSamplerFactory(
            BigtableSourceBuilder<String> builder, RowKeySamplerFactory samplerFactory) {
        return builder.samplerFactory(samplerFactory);
    }

    /**
     * Applies an opener seam to a builder, reaching the package-private setter for a subpackage.
     */
    public static BigtableSourceBuilder<String> withOpener(
            BigtableSourceBuilder<String> builder, RowStreamOpener opener) {
        return builder.opener(opener);
    }

    /** Lowers the fetch cap, so a checkpoint can land inside a range holding few rows. */
    public static BigtableSourceBuilder<String> withMaxRowsPerFetch(
            BigtableSourceBuilder<String> builder, int maxRowsPerFetch) {
        return builder.maxRowsPerFetch(maxRowsPerFetch);
    }

    /** Turns each row into its key, so a test can assert on plain strings. */
    public static final class RowKeyDeserializer
            implements BigtableRowDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(Row row, Collector<String> out) {
            out.collect(row.getKey().toStringUtf8());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }
}
