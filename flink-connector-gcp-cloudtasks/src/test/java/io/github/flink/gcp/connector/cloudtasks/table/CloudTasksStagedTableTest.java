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

package io.github.flink.gcp.connector.cloudtasks.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.InstantiationUtil;

import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksStagedCreateTaskSink;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksStagedOptions;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksStagedOptions.ExpiredEnvelopePolicy;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.cloudtasks.table.sink.CloudTasksDynamicSink;
import io.github.flink.gcp.connector.cloudtasks.table.sink.HttpTargetSpec;
import io.github.flink.gcp.connector.cloudtasks.table.sink.WriterOptionsMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CloudTasksStagedTableTest {
    private static final EncodingFormat<SerializationSchema<RowData>> FORMAT =
            new EncodingFormat<>() {
                @Override
                public SerializationSchema<RowData> createRuntimeEncoder(
                        DynamicTableSink.Context context, DataType type) {
                    throw new AssertionError("Equality does not create a runtime encoder");
                }

                @Override
                public ChangelogMode getChangelogMode() {
                    return ChangelogMode.insertOnly();
                }
            };

    private static DynamicTableSink comparisonSink(Map<String, String> options) {
        var config = Configuration.fromMap(options);
        return new CloudTasksDynamicSink(
                "catalog.database.tasks",
                SCHEMA.toPhysicalRowDataType(),
                FORMAT,
                QueueDestination.of("p", "l", "q"),
                HttpTargetSpec.from(config, null),
                false,
                WriterOptionsMapper.map(config),
                WriterOptionsMapper.deliveryGuarantee(config),
                WriterOptionsMapper.mapStaged(config),
                null,
                "127.0.0.1:1",
                3);
    }

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(Column.physical("payload", DataTypes.STRING()));

    static Map<String, String> options() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "cloud-tasks");
        options.put("project", "p");
        options.put("location", "l");
        options.put("queue", "q");
        options.put("format", "json");
        options.put("http.url", "https://example.com/task");
        options.put("emulator-endpoint", "127.0.0.1:1");
        options.put("sink.delivery-guarantee", "exactly-once");
        return options;
    }

    private static CloudTasksStagedCreateTaskSink<RowData> runtime(DynamicTableSink table) {
        return (CloudTasksStagedCreateTaskSink<RowData>)
                ((SinkV2Provider)
                                table.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false)))
                        .createSink();
    }

    @Test
    void mapsEveryStagingSettingAndPreservesThemAcrossCopyAndSerialization() throws Exception {
        var options = options();
        options.put("sink.staged.name-retention", "2 h");
        options.put("sink.staged.clock-skew-allowance", "1 min");
        options.put("sink.staged.request-timeout", "3 s");
        options.put("sink.staged.max-tasks", "17");
        options.put("sink.staged.max-bytes", "12345");
        options.put("sink.staged.verify-queue-retention", "false");
        options.put("sink.staged.expired-envelope-policy", "create-anyway");
        options.put("sink.in-flight.max-tasks", "2");
        options.put("sink.recovery.max-attempts", "4");
        options.put("sink.metrics.per-destination", "true");
        options.put("sink.parallelism", "3");
        var table = FactoryMocks.createTableSink(SCHEMA, options);
        var independentlyBuilt = comparisonSink(options);
        assertThat(comparisonSink(options))
                .isEqualTo(independentlyBuilt)
                .hasSameHashCodeAs(independentlyBuilt);
        assertThat(table.copy()).isEqualTo(table);
        var sink = runtime(table.copy());
        var restored = InstantiationUtil.clone(sink);
        var expected =
                CloudTasksStagedOptions.builder()
                        .nameRetention(Duration.ofHours(2))
                        .clockSkewAllowance(Duration.ofMinutes(1))
                        .requestTimeout(Duration.ofSeconds(3))
                        .maxStagedTasks(17)
                        .maxStagedBytes(12345)
                        .verifyQueueRetention(false)
                        .expiredEnvelopePolicy(ExpiredEnvelopePolicy.CREATE_ANYWAY)
                        .build();
        assertThat(restored.getStagedOptions()).isEqualTo(expected).hasSameHashCodeAs(expected);
        assertThat(restored.getConfig().getWriterOptions().getMaxInFlightTasks()).isEqualTo(2);
        assertThat(restored.getConfig().getWriterOptions().getRecoveryMaxAttempts()).isEqualTo(4);
        assertThat(restored.getConfig().getWriterOptions().isPerDestinationMetrics()).isTrue();
        assertThat(
                        ((SinkV2Provider)
                                        table.getSinkRuntimeProvider(
                                                new SinkRuntimeProviderContext(false)))
                                .getParallelism())
                .contains(3);
        assertThat(restored.getLineageVertex().datasets()).hasSize(1);
        assertThat(restored.getLineageVertex().datasets().get(0).name())
                .isEqualTo(sink.getLineageVertex().datasets().get(0).name());
        assertThat(runtime(FactoryMocks.createTableSink(SCHEMA, options())).getStagedOptions())
                .isEqualTo(CloudTasksStagedOptions.builder().build());
    }

    @ParameterizedTest
    @CsvSource({
        "sink.staged.name-retention,0 ms,nameRetention must be positive",
        "sink.staged.request-timeout,0 ms,requestTimeout must be positive",
        "sink.staged.max-tasks,0,maxStagedTasks must be positive",
        "sink.staged.max-bytes,0,maxStagedBytes must be positive",
        "sink.staged.name-retention,400000 d,nameRetention must be at most",
        "sink.staged.clock-skew-allowance,400000 d,clockSkewAllowance must be at most",
        "sink.staged.request-timeout,400000 d,requestTimeout must be at most"
    })
    void rejectsSingleValuesWithTheirSqlKeyAndBuilderReason(
            String key, String value, String reason) {
        var options = options();
        options.put(key, value);
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .hasStackTraceContaining("Option '" + key + "' is invalid")
                .hasStackTraceContaining(reason);
    }

    @Test
    void negativeDurationUsesTheParserDiagnosticOrTheTypedMapperDiagnostic() {
        var options = options();
        options.put("sink.staged.clock-skew-allowance", "-1 ms");
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .hasStackTraceContaining("sink.staged.clock-skew-allowance")
                .hasStackTraceContaining("Could not parse");
        var typed = Configuration.fromMap(options());
        typed.set(
                CloudTasksConnectorOptions.SINK_STAGED_CLOCK_SKEW_ALLOWANCE, Duration.ofMillis(-1));
        assertThatThrownBy(() -> WriterOptionsMapper.mapStaged(typed))
                .hasMessageContaining("Option 'sink.staged.clock-skew-allowance' is invalid")
                .hasMessageContaining("clockSkewAllowance must be non-negative");
    }

    @ParameterizedTest
    @CsvSource({
        "sink.staged.name-retention,1 h", "sink.staged.clock-skew-allowance,5 min",
        "sink.staged.request-timeout,20 s", "sink.staged.max-tasks,100000",
        "sink.staged.max-bytes,67108864", "sink.staged.verify-queue-retention,true",
        "sink.staged.expired-envelope-policy,fail"
    })
    void rejectsEveryExplicitStagingSettingEvenWhenItEqualsItsDefault(String key, String value) {
        var options = options();
        options.remove("sink.delivery-guarantee");
        options.put(key, value);
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .hasStackTraceContaining(key)
                .hasStackTraceContaining("sink.delivery-guarantee");
        options.put("sink.delivery-guarantee", "at-least-once");
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .hasStackTraceContaining(key)
                .hasStackTraceContaining("exactly-once");
    }

    @ParameterizedTest
    @CsvSource({"1 s,0 ms,1 s", "1000001 ns,0 ms,1000000 ns", "1 s,2 s,1 ms"})
    void namesAllSqlKeysForAnEmptyOrSubMillisecondWindow(
            String retention, String skew, String timeout) {
        var config = Configuration.fromMap(options());
        config.setString("sink.staged.name-retention", retention);
        config.setString("sink.staged.clock-skew-allowance", skew);
        config.setString("sink.staged.request-timeout", timeout);
        assertThatThrownBy(() -> WriterOptionsMapper.mapStaged(config))
                .hasMessageContaining("sink.staged.name-retention")
                .hasMessageContaining("sink.staged.clock-skew-allowance")
                .hasMessageContaining("sink.staged.request-timeout")
                .hasMessageContaining("one whole millisecond");
    }

    @ParameterizedTest
    @CsvSource({
        "fail,FAIL",
        "assume-committed,ASSUME_COMMITTED",
        "create-anyway,CREATE_ANYWAY",
        "drop,DROP"
    })
    void acceptsTheDocumentedExpiryVocabulary(String spelling, ExpiredEnvelopePolicy expected) {
        var options = options();
        options.put("sink.staged.expired-envelope-policy", spelling);
        assertThat(
                        runtime(FactoryMocks.createTableSink(SCHEMA, options))
                                .getStagedOptions()
                                .getExpiredEnvelopePolicy())
                .isEqualTo(expected);
    }

    @Test
    void boundedContextFailsWithTheDeliveryKeyButStreamingContextIsAccepted() {
        var table = FactoryMocks.createTableSink(SCHEMA, options());
        assertThatThrownBy(() -> table.getSinkRuntimeProvider(new SinkRuntimeProviderContext(true)))
                .hasMessageContaining("sink.delivery-guarantee")
                .hasMessageContaining("STREAMING");
        assertThat(runtime(table)).isNotNull();
    }

    @ParameterizedTest
    @CsvSource({
        "sink.staged.name-retention,2 h", "sink.staged.clock-skew-allowance,1 min",
        "sink.staged.request-timeout,5 s", "sink.staged.max-tasks,99",
        "sink.staged.max-bytes,999", "sink.staged.verify-queue-retention,false",
        "sink.staged.expired-envelope-policy,drop", "sink.delivery-guarantee,at-least-once"
    })
    void plannerEqualityDistinguishesEachSetting(String key, String value) {
        var changed = options();
        changed.put(key, value);
        assertThat(comparisonSink(options())).isNotEqualTo(comparisonSink(changed));
    }
}
