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

package io.github.flink.gcp.connector.spanner.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.util.Collector;

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.source.batch.SpannerBatchReadSource;
import io.github.flink.gcp.connector.spanner.source.batch.enumerator.PartitionPlannerFactory;
import io.github.flink.gcp.connector.spanner.source.batch.reader.StructStreamOpener;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerStructDeserializationSchema;

import java.util.function.UnaryOperator;

/**
 * Builds source configurations for tests in the source's subpackages, which cannot reach the
 * configuration's package-private constructor or the builder's test-only setters.
 */
public final class TestSources {

    /** The database every fixture reads. */
    public static final DatabaseDestination DATABASE = DatabaseDestination.of("p", "i", "db");

    /** The read every fixture asks for. */
    public static final SpannerReadOperation OPERATION =
            SpannerReadOperation.query(Statement.of("SELECT id FROM singers"));

    private TestSources() {}

    /**
     * Returns the configuration of a source built with the defaults.
     *
     * @return the configuration
     */
    public static SpannerSourceConfig<Long> config() {
        return config(UnaryOperator.identity());
    }

    /**
     * Returns the configuration of a source built with the given knobs applied.
     *
     * @param customizer applies the knobs a test needs
     * @return the configuration
     */
    public static SpannerSourceConfig<Long> config(
            UnaryOperator<SpannerSourceBuilder<Long>> customizer) {
        return source(customizer).getConfig();
    }

    /**
     * Returns the configuration of a Change Streams source through its test-only accessor.
     *
     * @param source the source
     * @return the configuration
     */
    public static SpannerChangeStreamSourceConfig<?> changeStreamConfig(Source<?, ?, ?> source) {
        return ((SpannerChangeStreamSource<?>) source).getConfig();
    }

    /**
     * Returns a source built with the given knobs applied, as its implementation type.
     *
     * @param customizer applies the knobs a test needs
     * @return the source
     */
    @SuppressWarnings("unchecked")
    public static SpannerBatchReadSource<Long> source(
            UnaryOperator<SpannerSourceBuilder<Long>> customizer) {
        SpannerSourceBuilder<Long> builder =
                SpannerSource.<Long>builder()
                        .database(DATABASE)
                        .readOperation(OPERATION)
                        .deserializer(new IdDeserializer())
                        // The builder creates this source's real clients, which would demand
                        // application-default credentials on a machine that has them and fail in CI
                        // on one that does not. The endpoint is never connected to.
                        .emulatorEndpoint("localhost:1");
        return (SpannerBatchReadSource<Long>) customizer.apply(builder).build();
    }

    /**
     * Applies a planner-factory seam to a builder, reaching the package-private setter for a
     * subpackage.
     *
     * @param builder the builder
     * @param plannerFactory the factory the source mints one planner per enumerator from
     * @return the builder
     */
    public static SpannerSourceBuilder<Long> withPlannerFactory(
            SpannerSourceBuilder<Long> builder, PartitionPlannerFactory plannerFactory) {
        return builder.plannerFactory(plannerFactory);
    }

    /**
     * Applies an opener seam to a builder, reaching the package-private setter for a subpackage.
     *
     * @param builder the builder
     * @param opener the opener
     * @return the builder
     */
    public static SpannerSourceBuilder<Long> withOpener(
            SpannerSourceBuilder<Long> builder, StructStreamOpener opener) {
        return builder.opener(opener);
    }

    /** A deserializer that reads the {@code id} column. */
    public static final class IdDeserializer implements SpannerStructDeserializationSchema<Long> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(Struct row, Collector<Long> out) {
            out.collect(row.getLong("id"));
        }

        @Override
        public TypeInformation<Long> getProducedType() {
            return TypeInformation.of(Long.class);
        }
    }
}
