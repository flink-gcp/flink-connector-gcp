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

import org.apache.flink.api.connector.source.Boundedness;

import io.github.flink.gcp.connector.firestore.source.batch.enumerator.DefaultQueryPlannerFactory;
import io.github.flink.gcp.connector.firestore.source.batch.reader.ClientQueryPageReader;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** What the builder accepts, refuses, and defaults. */
class FirestoreSourceBuilderTest {

    private static final FirestoreQueryFactory QUERY = firestore -> firestore.collection("c");

    @Test
    void defaultsAScanToTheParallelismAndTheServicesReadTime() {
        FirestoreSourceConfig<String> config = TestSources.scanConfig(UnaryOperator.identity());

        assertThat(config.getCollectionGroup()).isEqualTo("orders");
        assertThat(config.getQueryFactory()).isNull();
        assertThat(config.getFieldMask()).isEmpty();
        assertThat(config.getPartitionCount()).isNull();
        assertThat(config.getReadTime()).isNull();
        assertThat(config.getPageSize()).isEqualTo(FirestoreSourceBuilder.DEFAULT_PAGE_SIZE);
        assertThat(config.getPlannerFactory()).isInstanceOf(DefaultQueryPlannerFactory.class);
        assertThat(config.getPageReader()).isInstanceOf(ClientQueryPageReader.class);
        assertThat(
                        TestSources.source(builder -> builder.collectionGroup("orders"))
                                .getBoundedness())
                .isEqualTo(Boundedness.BOUNDED);
    }

    @Test
    void carriesEveryKnob() {
        Instant readTime = Instant.parse("2026-09-30T00:00:00Z");
        FirestoreSourceConfig<String> config =
                TestSources.scanConfig(
                        builder ->
                                builder.select("a", "b.c")
                                        .select("d")
                                        .partitionCount(12)
                                        .readTime(readTime)
                                        .pageSize(50));

        assertThat(config.getFieldMask()).containsExactly("a", "b.c", "d");
        assertThat(config.getPartitionCount()).isEqualTo(12);
        assertThat(config.getReadTime()).isEqualTo(readTime);
        assertThat(config.getPageSize()).isEqualTo(50);
    }

    @Test
    void readsAQueryInsteadOfAScan() {
        FirestoreSourceConfig<String> config =
                TestSources.source(builder -> builder.query(QUERY)).getConfig();

        assertThat(config.getCollectionGroup()).isNull();
        assertThat(config.getQueryFactory()).isSameAs(QUERY);
    }

    @Test
    void requiresTheDatabaseAndTheDeserializer() {
        assertThatThrownBy(
                        () ->
                                FirestoreSource.<String>builder()
                                        .collectionGroup("orders")
                                        .deserializer(new TestSources.DocumentPathDeserializer())
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("database");
        assertThatThrownBy(
                        () ->
                                FirestoreSource.<String>builder()
                                        .database(TestSources.DATABASE)
                                        .collectionGroup("orders")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("deserializer");
    }

    @Test
    void requiresExactlyOneOfACollectionGroupAndAQuery() {
        assertThatThrownBy(() -> TestSources.source(UnaryOperator.identity()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("exactly one of collectionGroup(...) and query(...)");
        assertThatThrownBy(
                        () ->
                                TestSources.source(
                                        builder -> builder.collectionGroup("o").query(QUERY)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("exactly one of collectionGroup(...) and query(...)");
    }

    @Test
    void refusesScanOnlyKnobsOnAQuery() {
        assertThatThrownBy(() -> TestSources.source(builder -> builder.query(QUERY).select("a")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("select(...)");
        assertThatThrownBy(
                        () -> TestSources.source(builder -> builder.query(QUERY).partitionCount(4)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("partitionCount(...)");
    }

    @Test
    void refusesACollectionGroupThatIsNotOnePathSegment() {
        FirestoreSourceBuilder<String> builder = FirestoreSource.builder();

        assertThatThrownBy(() -> builder.collectionGroup("a/b"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("collectionGroup");
        assertThatThrownBy(() -> builder.collectionGroup(" a"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.collectionGroup(""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void refusesNonPositiveCountsAndBlankFields() {
        FirestoreSourceBuilder<String> builder = FirestoreSource.builder();

        assertThatThrownBy(() -> builder.partitionCount(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.pageSize(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.select("a", " "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.readTime(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void refusesAKeyFileWithAnEmulator() {
        assertThatThrownBy(
                        () ->
                                TestSources.source(
                                        builder ->
                                                builder.collectionGroup("orders")
                                                        .serviceAccountKeyFile("/key.json")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot be combined with emulatorEndpoint");
    }
}
