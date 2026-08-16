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

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Statement;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SpannerReadOperation}. */
class SpannerReadOperationTest {

    private static final List<String> COLUMNS = Arrays.asList("id", "name");

    @Test
    void aQueryCarriesItsStatementAndNothingElse() {
        SpannerReadOperation operation =
                SpannerReadOperation.query(Statement.of("SELECT id FROM singers"));

        assertThat(operation.isQuery()).isTrue();
        assertThat(operation.getStatement().getSql()).isEqualTo("SELECT id FROM singers");
        // The read fields stay null, which is what the planner branches on: a value object that
        // carried both shapes would let a caller ask for a partitionQuery over a table name.
        assertThat(operation.getTable()).isNull();
        assertThat(operation.getIndex()).isNull();
        assertThat(operation.getKeys()).isNull();
        assertThat(operation.getColumns()).isNull();
    }

    @Test
    void aReadCarriesItsTableAndNoStatement() {
        SpannerReadOperation operation =
                SpannerReadOperation.read("singers", KeySet.all(), COLUMNS);

        assertThat(operation.isQuery()).isFalse();
        assertThat(operation.getStatement()).isNull();
        assertThat(operation.getTable()).isEqualTo("singers");
        assertThat(operation.getIndex()).isNull();
        assertThat(operation.getKeys().isAll()).isTrue();
        assertThat(operation.getColumns()).containsExactly("id", "name");
    }

    @Test
    void anIndexReadCarriesTheIndex() {
        SpannerReadOperation operation =
                SpannerReadOperation.readUsingIndex(
                        "singers", "singers_by_name", KeySet.all(), COLUMNS);

        assertThat(operation.getIndex()).isEqualTo("singers_by_name");
    }

    @Test
    void theColumnsAreCopiedAndUnmodifiable() {
        List<String> columns = new java.util.ArrayList<>(COLUMNS);
        SpannerReadOperation operation =
                SpannerReadOperation.read("singers", KeySet.all(), columns);

        columns.add("extra");

        // Copied on the way in, so a caller reusing its list cannot change a running job's read;
        // and unmodifiable on the way out, so nothing downstream can either.
        assertThat(operation.getColumns()).containsExactly("id", "name");
        assertThatThrownBy(() -> operation.getColumns().add("extra"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void aReadWithoutColumnsIsRejected() {
        // Rejected rather than passed on: Spanner refuses it too, and refusing here names the
        // builder call rather than a partition request nobody wrote.
        assertThatThrownBy(
                        () ->
                                SpannerReadOperation.read(
                                        "singers", KeySet.all(), Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("columns must not be empty");
    }

    @Test
    void blankNamesAreRejected() {
        assertThatThrownBy(() -> SpannerReadOperation.read(" ", KeySet.all(), COLUMNS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("table must not be blank");
        assertThatThrownBy(
                        () ->
                                SpannerReadOperation.readUsingIndex(
                                        "singers", " ", KeySet.all(), COLUMNS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("index must not be blank");
        assertThatThrownBy(
                        () ->
                                SpannerReadOperation.read(
                                        "singers", KeySet.all(), Arrays.asList("id", " ")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column must not be blank");
    }

    @Test
    void nullsAreRejected() {
        assertThatThrownBy(() -> SpannerReadOperation.query(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> SpannerReadOperation.read("singers", null, COLUMNS))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> SpannerReadOperation.read("singers", KeySet.all(), null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(
                        () ->
                                SpannerReadOperation.readUsingIndex(
                                        "singers", null, KeySet.all(), COLUMNS))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void twoOperationsOfTheSameShapeAreEqual() {
        assertThat(SpannerReadOperation.query(Statement.of("SELECT 1")))
                .isEqualTo(SpannerReadOperation.query(Statement.of("SELECT 1")))
                .hasSameHashCodeAs(SpannerReadOperation.query(Statement.of("SELECT 1")))
                .isNotEqualTo(SpannerReadOperation.query(Statement.of("SELECT 2")))
                .isNotEqualTo(SpannerReadOperation.read("singers", KeySet.all(), COLUMNS));
    }

    @Test
    void theRenderingNamesWhatIsRead() {
        // Read back in the enumerator's log lines and in its planning-failure message, so a user
        // meeting "Spanner will not partition this" can see which read it means.
        assertThat(SpannerReadOperation.query(Statement.of("SELECT id FROM singers")).toString())
                .isEqualTo("query [SELECT id FROM singers]");
        assertThat(SpannerReadOperation.read("singers", KeySet.all(), COLUMNS).toString())
                .isEqualTo("read of table singers columns [id, name]");
        assertThat(
                        SpannerReadOperation.readUsingIndex(
                                        "singers", "by_name", KeySet.all(), COLUMNS)
                                .toString())
                .isEqualTo("read of table singers through index by_name columns [id, name]");
    }

    @Test
    void aDeferredOperationResolvesAtTheSuppliedSnapshot() throws Exception {
        DatabaseClient client =
                (DatabaseClient)
                        Proxy.newProxyInstance(
                                DatabaseClient.class.getClassLoader(),
                                new Class<?>[] {DatabaseClient.class},
                                (proxy, method, arguments) -> {
                                    throw new AssertionError(
                                            "The resolver must not call " + method);
                                });
        Timestamp readTimestamp = Timestamp.ofTimeSecondsAndNanos(123, 456);
        SpannerReadOperation concrete = SpannerReadOperation.read("singers", KeySet.all(), COLUMNS);
        SpannerReadOperation deferred =
                SpannerReadOperationResolution.deferred(
                        (seenClient, seenTimestamp) -> {
                            assertThat(seenClient).isSameAs(client);
                            assertThat(seenTimestamp).isEqualTo(readTimestamp);
                            return concrete;
                        });

        assertThat(SpannerReadOperationResolution.resolve(deferred, client, readTimestamp))
                .isSameAs(concrete);
        assertThat(SpannerReadOperationResolution.resolve(concrete, client, readTimestamp))
                .isSameAs(concrete);
    }
}
