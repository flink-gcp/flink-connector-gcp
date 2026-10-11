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

package io.github.flink.gcp.connector.datastore.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import com.google.cloud.datastore.Entity;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.source.batch.DatastoreBatchSource;
import io.github.flink.gcp.connector.datastore.source.batch.enumerator.QueryPlannerFactory;
import io.github.flink.gcp.connector.datastore.source.batch.reader.QueryPageReader;
import io.github.flink.gcp.connector.datastore.source.serializer.DatastoreEntityDeserializationSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

/** Builds sources and reaches the builder's test seams, for the source's tests. */
public final class TestSources {

    /** The database every unit fixture reads. */
    public static final DatabaseDestination DATABASE = DatabaseDestination.of("p");

    private TestSources() {}

    /**
     * Returns a source with the given knobs applied, as its implementation type.
     *
     * @param customizer applies the knobs a test needs, the read shape among them
     * @return the source
     */
    public static DatastoreBatchSource<String> source(
            UnaryOperator<DatastoreSourceBuilder<String>> customizer) {
        DatastoreSourceBuilder<String> builder =
                DatastoreSource.<String>builder()
                        .database(DATABASE)
                        .deserializer(new KeyNameDeserializer())
                        // The builder creates this source's real clients, which would demand
                        // application-default credentials on a machine that has them. The endpoint
                        // is never connected to.
                        .emulatorEndpoint("localhost:1");
        return (DatastoreBatchSource<String>) customizer.apply(builder).build();
    }

    /** Returns the configuration of a scan of kind {@code Task} with the given knobs applied. */
    public static DatastoreSourceConfig<String> kindConfig(
            UnaryOperator<DatastoreSourceBuilder<String>> customizer) {
        return source(builder -> customizer.apply(builder.kind("Task"))).getConfig();
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
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "datastore")
                        .executeAndCollect()) {
            collected.forEachRemaining(records::add);
        }
        return records;
    }

    /** Replaces the builder's planner factory, which is package-private. */
    public static <T> DatastoreSourceBuilder<T> withPlannerFactory(
            DatastoreSourceBuilder<T> builder, QueryPlannerFactory factory) {
        return builder.plannerFactory(factory);
    }

    /** Replaces the builder's page reader, which is package-private. */
    public static <T> DatastoreSourceBuilder<T> withPageReader(
            DatastoreSourceBuilder<T> builder, QueryPageReader pageReader) {
        return builder.pageReader(pageReader);
    }

    /** Emits each entity's key name. */
    public static final class KeyNameDeserializer
            implements DatastoreEntityDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(Entity entity, Collector<String> out) {
            out.collect(entity.getKey().getName());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }
}
