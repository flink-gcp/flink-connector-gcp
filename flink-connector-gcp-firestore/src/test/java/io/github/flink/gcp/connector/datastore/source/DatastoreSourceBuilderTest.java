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

import org.apache.flink.api.connector.source.Boundedness;

import com.google.datastore.v1.Query;
import com.google.protobuf.Int32Value;
import io.github.flink.gcp.connector.datastore.source.batch.SplittableQueries;
import io.github.flink.gcp.connector.datastore.source.batch.enumerator.DefaultQueryPlannerFactory;
import io.github.flink.gcp.connector.datastore.source.batch.reader.ClientQueryPageReader;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** What the builder accepts, refuses, and defaults. */
class DatastoreSourceBuilderTest {

    private static final Query LIMITED =
            SplittableQueries.ofKind("Task").toBuilder().setLimit(Int32Value.of(5)).build();

    @Test
    void defaultsAKindScanToAnEstimatedCountAndTheServicesReadTime() {
        DatastoreSourceConfig<String> config = TestSources.kindConfig(UnaryOperator.identity());

        assertThat(config.getQuery()).isEqualTo(SplittableQueries.ofKind("Task"));
        assertThat(config.getGqlQuery()).isNull();
        assertThat(config.getNamespace()).isEmpty();
        assertThat(config.getSplitCount()).isNull();
        assertThat(config.getReadTime()).isNull();
        assertThat(config.getPageSize()).isEqualTo(DatastoreSourceBuilder.DEFAULT_PAGE_SIZE);
        assertThat(config.getPlannerFactory()).isInstanceOf(DefaultQueryPlannerFactory.class);
        assertThat(config.getPageReader()).isInstanceOf(ClientQueryPageReader.class);
        assertThat(TestSources.source(builder -> builder.kind("Task")).getBoundedness())
                .isEqualTo(Boundedness.BOUNDED);
    }

    @Test
    void carriesEveryKnob() {
        Instant readTime = Instant.parse("2026-10-03T00:00:00Z");
        DatastoreSourceConfig<String> config =
                TestSources.kindConfig(
                        builder ->
                                builder.namespace("tenant")
                                        .splitCount(64)
                                        .readTime(readTime)
                                        .pageSize(50));

        assertThat(config.getNamespace()).isEqualTo("tenant");
        assertThat(config.getSplitCount()).isEqualTo(64);
        assertThat(config.getReadTime()).isEqualTo(readTime);
        assertThat(config.getPageSize()).isEqualTo(50);
    }

    @Test
    void readsAQueryOrAGqlQueryInsteadOfAKind() {
        DatastoreSourceConfig<String> query =
                TestSources.source(builder -> builder.query(LIMITED)).getConfig();
        DatastoreSourceConfig<String> gql =
                TestSources.source(builder -> builder.gqlQuery("SELECT * FROM Task")).getConfig();

        assertThat(query.getQuery()).isSameAs(LIMITED);
        assertThat(query.getGqlQuery()).isNull();
        assertThat(gql.getQuery()).isNull();
        assertThat(gql.getGqlQuery()).isEqualTo("SELECT * FROM Task");
    }

    @Test
    void requiresTheDatabaseAndTheDeserializer() {
        assertThatThrownBy(
                        () ->
                                DatastoreSource.<String>builder()
                                        .kind("Task")
                                        .deserializer(new TestSources.KeyNameDeserializer())
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("database");
        assertThatThrownBy(
                        () ->
                                DatastoreSource.<String>builder()
                                        .database(TestSources.DATABASE)
                                        .kind("Task")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("deserializer");
    }

    @Test
    void requiresExactlyOneReadShape() {
        String message = "exactly one of kind(...), query(...) and gqlQuery(...)";
        assertThatThrownBy(() -> TestSources.source(UnaryOperator.identity()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(message);
        assertThatThrownBy(() -> TestSources.source(builder -> builder.kind("K").query(LIMITED)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(message);
        assertThatThrownBy(
                        () ->
                                TestSources.source(
                                        builder -> builder.query(LIMITED).gqlQuery("SELECT *")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(message);
    }

    @Test
    void refusesASplitCountForAQueryThatCannotBeSplit() {
        assertThatThrownBy(
                        () -> TestSources.source(builder -> builder.query(LIMITED).splitCount(4)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("splitCount(...)")
                .hasMessageContaining("because it has a limit");
    }

    @Test
    void takesASplitCountForAGqlQueryWhichIsCheckedWhenItIsParsed() {
        assertThat(
                        TestSources.source(
                                        builder ->
                                                builder.gqlQuery("SELECT * FROM Task")
                                                        .splitCount(4))
                                .getConfig()
                                .getSplitCount())
                .isEqualTo(4);
    }

    @Test
    void refusesBlankNamesAndNonPositiveCounts() {
        DatastoreSourceBuilder<String> builder = DatastoreSource.builder();

        assertThatThrownBy(() -> builder.kind(" ")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.gqlQuery("")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.namespace(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("leave it unset");
        assertThatThrownBy(() -> builder.splitCount(0))
                .isInstanceOf(IllegalArgumentException.class);
        // The splitter would size an array and an int limit from it before any check of its own.
        assertThatThrownBy(() -> builder.splitCount(DatastoreSourceBuilder.MAX_SPLIT_COUNT + 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("between 1 and 50000");
        assertThat(builder.splitCount(DatastoreSourceBuilder.MAX_SPLIT_COUNT)).isSameAs(builder);
        assertThatThrownBy(() -> builder.pageSize(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.query(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.readTime(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void refusesANearestNeighbourSearch() {
        Query nearest =
                SplittableQueries.ofKind("Task").toBuilder()
                        .setFindNearest(com.google.datastore.v1.FindNearest.newBuilder())
                        .build();

        assertThatThrownBy(() -> DatastoreSource.<String>builder().query(nearest))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nearest-neighbour search");
    }

    @Test
    void refusesAKeyFileWithAnEmulator() {
        assertThatThrownBy(
                        () ->
                                TestSources.source(
                                        builder ->
                                                builder.kind("Task")
                                                        .serviceAccountKeyFile("/key.json")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot be combined with emulatorEndpoint");
    }
}
