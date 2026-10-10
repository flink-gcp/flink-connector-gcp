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

package io.github.flink.gcp.connector.firestore.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import com.google.cloud.firestore.DocumentSnapshot;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.source.batch.FirestoreBatchSource;
import io.github.flink.gcp.connector.firestore.source.batch.enumerator.QueryPlannerFactory;
import io.github.flink.gcp.connector.firestore.source.batch.reader.QueryPageReader;
import io.github.flink.gcp.connector.firestore.source.serializer.FirestoreDocumentDeserializationSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

/** Builds sources and reaches the builder's test seams, for the source's tests. */
public final class TestSources {

    /** The database every unit fixture reads. */
    public static final DatabaseDestination DATABASE = DatabaseDestination.of("p");

    private TestSources() {}

    /**
     * Returns a collection-group scan source with the given knobs applied, as its implementation
     * type.
     *
     * @param customizer applies the knobs a test needs
     * @return the source
     */
    public static FirestoreBatchSource<String> source(
            UnaryOperator<FirestoreSourceBuilder<String>> customizer) {
        FirestoreSourceBuilder<String> builder =
                FirestoreSource.<String>builder()
                        .database(DATABASE)
                        .deserializer(new DocumentPathDeserializer())
                        // The builder creates this source's real clients, which would demand
                        // application-default credentials on a machine that has them. The endpoint
                        // is never connected to.
                        .emulatorEndpoint("localhost:1");
        return (FirestoreBatchSource<String>) customizer.apply(builder).build();
    }

    /** Returns the configuration of a scan of {@code orders} with the given knobs applied. */
    public static FirestoreSourceConfig<String> scanConfig(
            UnaryOperator<FirestoreSourceBuilder<String>> customizer) {
        return source(builder -> customizer.apply(builder.collectionGroup("orders"))).getConfig();
    }

    /**
     * Runs the source in a local job of the given parallelism and returns every record it emits.
     *
     * @param source the source to read
     * @param parallelism the job's parallelism
     * @return the records, in the order the job delivered them
     */
    public static <T> List<T> collect(Source<T, ?, ?> source, int parallelism) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(new Configuration());
        env.setParallelism(parallelism);
        List<T> records = new ArrayList<>();
        try (CloseableIterator<T> collected =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "firestore")
                        .executeAndCollect()) {
            collected.forEachRemaining(records::add);
        }
        return records;
    }

    /** Replaces the builder's planner factory, which is package-private. */
    public static <T> FirestoreSourceBuilder<T> withPlannerFactory(
            FirestoreSourceBuilder<T> builder, QueryPlannerFactory factory) {
        return builder.plannerFactory(factory);
    }

    /** Replaces the builder's page reader, which is package-private. */
    public static <T> FirestoreSourceBuilder<T> withPageReader(
            FirestoreSourceBuilder<T> builder, QueryPageReader pageReader) {
        return builder.pageReader(pageReader);
    }

    /** Emits each document's path. */
    public static final class DocumentPathDeserializer
            implements FirestoreDocumentDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(DocumentSnapshot document, Collector<String> out) {
            out.collect(document.getReference().getPath());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }
}
