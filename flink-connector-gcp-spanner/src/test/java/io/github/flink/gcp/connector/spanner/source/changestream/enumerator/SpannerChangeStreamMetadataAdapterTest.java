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

package io.github.flink.gcp.connector.spanner.source.changestream.enumerator;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannerChangeStreamMetadataAdapterTest {

    private static final Type OPTION_ROW =
            Type.struct(Type.StructField.of("option_value", Type.string()));

    @Test
    void buildsDialectSpecificParameterizedQueries() {
        Statement google =
                SpannerChangeStreamMetadataAdapter.optionQuery(
                        Dialect.GOOGLE_STANDARD_SQL, "orders", "retention_period");
        Statement postgres =
                SpannerChangeStreamMetadataAdapter.optionQuery(
                        Dialect.POSTGRESQL, "orders", "partition_mode");

        assertThat(google.getSql()).contains("INFORMATION_SCHEMA.CHANGE_STREAM_OPTIONS");
        assertThat(google.getSql()).contains("@stream_name").contains("@option_name");
        assertThat(postgres.getSql())
                .contains("information_schema.change_stream_options")
                .contains("change_stream_schema = 'public'")
                .contains("change_stream_name = $1")
                .contains("option_name = $2");
        assertThat(google.getParameters().get("stream_name").getString()).isEqualTo("orders");
        assertThat(postgres.getParameters().get("p1").getString()).isEqualTo("orders");
        assertThat(postgres.getParameters().get("p2").getString()).isEqualTo("partition_mode");
    }

    @Test
    void parsesEveryDocumentedRetentionUnitAndUsesTheAbsentRowFallback() throws Exception {
        assertThat(SpannerChangeStreamMetadataAdapter.parseDuration("7d"))
                .isEqualTo(Duration.ofDays(7));
        assertThat(SpannerChangeStreamMetadataAdapter.parseDuration("36H"))
                .isEqualTo(Duration.ofHours(36));
        assertThat(SpannerChangeStreamMetadataAdapter.parseDuration("90m"))
                .isEqualTo(Duration.ofMinutes(90));
        assertThat(SpannerChangeStreamMetadataAdapter.parseDuration("3600s"))
                .isEqualTo(Duration.ofHours(1));

        SpannerChangeStreamMetadataAdapter adapter =
                adapter(Dialect.GOOGLE_STANDARD_SQL, statement -> rows());
        assertThat(adapter.retention()).isEqualTo(Duration.ofDays(7));

        SpannerChangeStreamMetadataAdapter configured =
                adapter(Dialect.GOOGLE_STANDARD_SQL, statement -> rows("36h"));
        assertThat(configured.retention()).isEqualTo(Duration.ofHours(36));
    }

    @Test
    void rejectsMalformedRetentionAndMutableOrUnknownPartitionModes() {
        assertThatThrownBy(() -> SpannerChangeStreamMetadataAdapter.parseDuration("PT24H"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive integer");

        SpannerChangeStreamMetadataAdapter mutable =
                adapter(Dialect.GOOGLE_STANDARD_SQL, statement -> rows("MUTABLE_KEY_RANGE"));
        assertThatThrownBy(mutable::validatePartitionMode)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("MUTABLE_KEY_RANGE")
                .hasMessageContaining("partition start, end, move-in, or move-out");

        SpannerChangeStreamMetadataAdapter unknown =
                adapter(Dialect.GOOGLE_STANDARD_SQL, statement -> rows("NEW_MODE"));
        assertThatThrownBy(unknown::validatePartitionMode)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("NEW_MODE");
    }

    @Test
    void immutableAndAbsentPartitionModesAreAcceptedAndCloseReleasesTheHandle() throws Exception {
        AtomicInteger closes = new AtomicInteger();
        List<ResultSet> answers = new ArrayList<>();
        answers.add(rows("IMMUTABLE_KEY_RANGE"));
        answers.add(rows());
        SpannerChangeStreamMetadataAdapter adapter =
                new SpannerChangeStreamMetadataAdapter(
                        "db",
                        "orders",
                        Duration.ofDays(7),
                        () -> Dialect.POSTGRESQL,
                        statement -> answers.remove(0),
                        closes::incrementAndGet);

        adapter.validatePartitionMode();
        adapter.validatePartitionMode();
        adapter.close();

        assertThat(closes.get()).isEqualTo(1);
    }

    private static SpannerChangeStreamMetadataAdapter adapter(
            Dialect dialect, java.util.function.Function<Statement, ResultSet> query) {
        return new SpannerChangeStreamMetadataAdapter(
                "projects/p/instances/i/databases/d",
                "orders",
                Duration.ofDays(7),
                () -> dialect,
                query,
                () -> {});
    }

    private static ResultSet rows(String... values) {
        List<Struct> rows = new ArrayList<>();
        for (String value : values) {
            rows.add(Struct.newBuilder().set("option_value").to(value).build());
        }
        return ResultSets.forRows(OPTION_ROW, rows);
    }
}
