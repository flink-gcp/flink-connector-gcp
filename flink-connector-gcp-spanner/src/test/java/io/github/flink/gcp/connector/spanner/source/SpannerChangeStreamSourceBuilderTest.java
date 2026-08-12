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

package io.github.flink.gcp.connector.spanner.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.util.InstantiationUtil;

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.changestream.Mod;
import io.github.flink.gcp.connector.spanner.source.changestream.ModType;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamRecordFilter;
import io.github.flink.gcp.connector.spanner.source.changestream.ValueCaptureType;
import io.github.flink.gcp.connector.spanner.source.changestream.reader.DefaultSpannerChangeStreamQueryClientFactory;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannerChangeStreamSourceBuilderTest {

    private static final SpannerDatabase DATABASE =
            SpannerDatabase.of("project", "instance", "database");

    @Test
    void defaultsAndEveryOptionReachTheConfiguration() {
        SpannerChangeStreamSourceConfig<Long> defaults = config(builder());

        assertThat(defaults.getDatabase()).isEqualTo(DATABASE);
        assertThat(defaults.getChangeStreamName()).isEqualTo("changes");
        assertThat(defaults.getStartPosition()).isEqualTo(StartPosition.latest());
        assertThat(defaults.getResumeFallback()).isEmpty();
        assertThat(defaults.getAbsentRetentionFallback()).isEqualTo(Duration.ofDays(7));
        assertThat(defaults.getHeartbeatMillis()).isEqualTo(2_000);
        assertThat(defaults.getRpcPriority()).isEqualTo(SpannerRpcPriority.HIGH);
        assertThat(defaults.getMaxConcurrentQueriesPerSubtask()).isEqualTo(8);
        DataChangeRecord unfiltered = record("orders");
        assertThat(defaults.getRecordFilter().filter(unfiltered).getRecord()).isSameAs(unfiltered);
        assertThat(defaults.getQueryClientFactory())
                .isInstanceOf(DefaultSpannerChangeStreamQueryClientFactory.class);

        SpannerChangeStreamSourceConfig<Long> configured =
                config(
                        builder()
                                .startPosition(StartPosition.earliest())
                                .resumeFallback(StartPosition.latest())
                                .absentRetentionFallback(Duration.ofDays(3))
                                .heartbeatInterval(Duration.ofMillis(1_500))
                                .rpcPriority(SpannerRpcPriority.LOW)
                                .maxConcurrentQueriesPerSubtask(19)
                                .tableIncludeList(Collections.singletonList("orders"))
                                .columnExcludeList(Collections.singletonList("orders\\.secret"))
                                .skipMessagesWithoutChange(true));

        assertThat(configured.getStartPosition()).isEqualTo(StartPosition.earliest());
        assertThat(configured.getResumeFallback()).contains(StartPosition.latest());
        assertThat(configured.getAbsentRetentionFallback()).isEqualTo(Duration.ofDays(3));
        assertThat(configured.getHeartbeatMillis()).isEqualTo(1_500);
        assertThat(configured.getRpcPriority()).isEqualTo(SpannerRpcPriority.LOW);
        assertThat(configured.getMaxConcurrentQueriesPerSubtask()).isEqualTo(19);
        assertThat(configured.getRecordFilter().filter(record("orders")).getDisposition())
                .isEqualTo(
                        SpannerChangeStreamRecordFilter.Result.Disposition.SKIPPED_WITHOUT_CHANGE);
        assertThat(configured.getRecordFilter().filter(record("audit")).getDisposition())
                .isEqualTo(SpannerChangeStreamRecordFilter.Result.Disposition.TABLE_FILTERED);
    }

    @Test
    void sourceIsUnboundedAndDeclaresTheDeserializerType() {
        SpannerChangeStreamSource<Long> source = builder().build();

        assertThat(source.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
        assertThat(source.getProducedType()).isEqualTo(TypeInformation.of(Long.class));
    }

    @Test
    void sourceAndItsFactoriesSurviveJobGraphSerialization() throws Exception {
        SpannerChangeStreamSource<Long> copy =
                InstantiationUtil.clone(
                        builder()
                                .tableIncludeList(Collections.singletonList("orders"))
                                .columnExcludeList(Collections.singletonList("orders\\.secret"))
                                .skipMessagesWithoutChange(true)
                                .build());

        assertThat(copy.getConfig().getDatabase()).isEqualTo(DATABASE);
        assertThat(copy.getConfig().getChangeStreamName()).isEqualTo("changes");
        assertThat(copy.getProducedType()).isEqualTo(TypeInformation.of(Long.class));
        assertThat(copy.getConfig().getRecordFilter().filter(record("orders")).getDisposition())
                .isEqualTo(
                        SpannerChangeStreamRecordFilter.Result.Disposition.SKIPPED_WITHOUT_CHANGE);
        assertThat(copy.getConfig().getRecordFilter().filter(record("audit")).getDisposition())
                .isEqualTo(SpannerChangeStreamRecordFilter.Result.Disposition.TABLE_FILTERED);
    }

    @Test
    void tableExcludeListReachesTheConfiguration() {
        SpannerChangeStreamSourceConfig<Long> configured =
                config(builder().tableExcludeList(Collections.singletonList("audit")));

        assertThat(configured.getRecordFilter().filter(record("orders")).getDisposition())
                .isEqualTo(SpannerChangeStreamRecordFilter.Result.Disposition.DELIVER);
        assertThat(configured.getRecordFilter().filter(record("audit")).getDisposition())
                .isEqualTo(SpannerChangeStreamRecordFilter.Result.Disposition.TABLE_FILTERED);
    }

    @Test
    void missingRequiredOptionsNameTheirSetters() {
        assertThatThrownBy(() -> SpannerChangeStreamSource.<Long>builder().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("database(...)");
        assertThatThrownBy(
                        () -> SpannerChangeStreamSource.<Long>builder().database(DATABASE).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("changeStreamName(...)");
        assertThatThrownBy(
                        () ->
                                SpannerChangeStreamSource.<Long>builder()
                                        .database(DATABASE)
                                        .changeStreamName("changes")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("deserializer(...)");
    }

    @Test
    void invalidBoundsFailAtTheSetter() {
        assertThatThrownBy(() -> builder().heartbeatInterval(Duration.ofMillis(999)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder().heartbeatInterval(Duration.ofMillis(300_001)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder().heartbeatInterval(Duration.ofSeconds(1).plusNanos(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("whole milliseconds");
        assertThatThrownBy(() -> builder().maxConcurrentQueriesPerSubtask(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
        assertThatThrownBy(() -> builder().absentRetentionFallback(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retention safety margin");
        assertThatThrownBy(() -> builder().absentRetentionFallback(Duration.ofMinutes(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retention safety margin");
    }

    @Test
    void invalidPatternsFailAtTheirSetterAndCollectionsAreDefensivelyCopied() {
        assertThatThrownBy(() -> builder().tableIncludeList(Collections.singletonList("[broken")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tableIncludeList")
                .hasMessageContaining("index 0");

        List<String> patterns = new ArrayList<>(Collections.singletonList("orders"));
        SpannerChangeStreamSourceBuilder<Long> configured = builder().tableIncludeList(patterns);
        patterns.set(0, "audit");

        assertThat(config(configured).getRecordFilter().filter(record("orders")).getDisposition())
                .isEqualTo(SpannerChangeStreamRecordFilter.Result.Disposition.DELIVER);
    }

    @Test
    void includeAndExcludeListsForTheSameScopeAreMutuallyExclusiveAtBuild() {
        assertThatThrownBy(
                        () ->
                                builder()
                                        .tableIncludeList(Collections.singletonList("orders"))
                                        .tableExcludeList(Collections.singletonList("audit"))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("tableIncludeList(...)")
                .hasMessageContaining("tableExcludeList(...)");
        assertThatThrownBy(
                        () ->
                                builder()
                                        .columnIncludeList(
                                                Collections.singletonList("orders\\.visible"))
                                        .columnExcludeList(
                                                Collections.singletonList("orders\\.secret"))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("columnIncludeList(...)")
                .hasMessageContaining("columnExcludeList(...)");
    }

    @Test
    void namesAndNullsAreRejectedAtTheirSetter() {
        assertThatThrownBy(() -> builder().changeStreamName(" "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder().changeStreamName(" changes"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder().database(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder().deserializer(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder().startPosition(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder().resumeFallback(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder().heartbeatInterval(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder().rpcPriority(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder().tableIncludeList(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(
                        () -> builder().columnExcludeList(Arrays.asList("orders\\.secret", null)))
                .isInstanceOf(NullPointerException.class);
    }

    private static SpannerChangeStreamSourceBuilder<Long> builder() {
        return SpannerChangeStreamSource.<Long>builder()
                .database(DATABASE)
                .changeStreamName("changes")
                .deserializer(new CommitSecondDeserializer())
                .emulatorEndpoint("localhost:1");
    }

    private static SpannerChangeStreamSourceConfig<Long> config(
            SpannerChangeStreamSourceBuilder<Long> builder) {
        return builder.build().getConfig();
    }

    private static DataChangeRecord record(String table) {
        return new DataChangeRecord(
                Instant.parse("2026-08-12T00:00:00Z"),
                "1",
                "tx",
                true,
                table,
                Arrays.asList(
                        new DataChangeRecord.ColumnType("id", "{\"code\":\"INT64\"}", true, 1),
                        new DataChangeRecord.ColumnType(
                                "secret", "{\"code\":\"STRING\"}", false, 2)),
                Collections.singletonList(new Mod("{\"id\":1}", "{\"secret\":\"hidden\"}", null)),
                ModType.UPDATE,
                ValueCaptureType.NEW_VALUES,
                1,
                1,
                "",
                false);
    }

    private static final class CommitSecondDeserializer
            implements SpannerChangeStreamDeserializationSchema<Long> {

        private static final long serialVersionUID = 1L;

        @Override
        public Long deserialize(DataChangeRecord record) {
            return record.getCommitTimestamp().getEpochSecond();
        }

        @Override
        public TypeInformation<Long> getProducedType() {
            return TypeInformation.of(Long.class);
        }
    }
}
