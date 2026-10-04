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

package io.github.flink.gcp.connector.tier3.bigquery;

import org.apache.flink.util.InstantiationUtil;

import io.github.flink.gcp.connector.bigquery.sink.WriteDisposition;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecoveryOptionsTest {
    static String[] arguments(String... overrides) {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("--run-id", "bigquery-it");
        values.put("--mode", "EO");
        values.put("--destinations", "10");
        for (int i = 0; i < overrides.length; i += 2) {
            values.put(overrides[i], overrides[i + 1]);
        }
        return values.entrySet().stream()
                .flatMap(e -> java.util.stream.Stream.of(e.getKey(), e.getValue()))
                .toArray(String[]::new);
    }

    @ParameterizedTest
    @EnumSource(RecoveryOptions.Mode.class)
    void defaultInputIsThirtyMinutesAtOneMiBPerSecond(RecoveryOptions.Mode mode) {
        var options = RecoveryOptions.parse(arguments("--mode", mode.name()));
        assertThat(options.records * mode.rowBytes).isEqualTo(1800L * 1024 * 1024);
        assertThat(options.bytesPerSecond).isEqualTo(1024 * 1024);
        assertThat(options.records * mode.rowBytes).isLessThan(RecoveryOptions.MAX_BYTES);
    }

    @ParameterizedTest
    @CsvSource({
        "destinations,9",
        "destinations,51",
        "records,19",
        "records,2097153",
        "records,9223372036854775807",
        "bytes-per-second,0",
        "bytes-per-second,1048577",
        "mode,OTHER",
        "staging-format,AVRO",
        "max-concurrent-checkpoint-finalizations,1",
        "max-concurrent-destinations,8",
        "max-staging-file-bytes,16777216",
        "max-open-destinations,16",
        "phase,other",
        "phase,upgrade",
        "require-restored,yes",
        "run-id,invalid_id",
        "run-id,/path",
        "extra,value"
    })
    void rejectsInvalidOrUnboundedInputs(String name, String value) {
        assertThatThrownBy(() -> RecoveryOptions.parse(arguments("--" + name, value)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @CsvSource({
        "staging-format,ORC",
        "max-concurrent-checkpoint-finalizations,one",
        "max-staging-file-bytes,16MiB"
    })
    void rejectsUnparseableFileLoadsArguments(String name, String value) {
        assertThatThrownBy(
                        () ->
                                RecoveryOptions.parse(
                                        arguments("--mode", "FILE_LOADS", "--" + name, value)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void fileLoadsDefaultsAreTheConnectorsAndStageInsideTheRunPrefix() {
        var options = RecoveryOptions.parse(arguments("--mode", "FILE_LOADS"));
        assertThat(options.records).isEqualTo(1843200);
        assertThat(options.checkpointInterval)
                .isEqualTo(Duration.ofSeconds(120))
                .isGreaterThanOrEqualTo(FileLoadsOptions.DEFAULT_MIN_CHECKPOINT_INTERVAL);
        var sink = options.fileLoads;
        var defaults = FileLoadsOptions.builder().stagingPath("gs://bucket").build();
        assertThat(sink.getStagingPath())
                .isEqualTo("gs://flink-gcp-tier3-bigquery/runs/bigquery-it/staging");
        assertThat(sink.getWriteDisposition()).isEqualTo(WriteDisposition.WRITE_APPEND);
        assertThat(sink.getMinCheckpointInterval()).isEqualTo(defaults.getMinCheckpointInterval());
        assertThat(sink.getStagingFormat()).isEqualTo(defaults.getStagingFormat());
        assertThat(sink.getMaxConcurrentCheckpointFinalizations())
                .isEqualTo(defaults.getMaxConcurrentCheckpointFinalizations());
        assertThat(sink.getMaxConcurrentDestinations())
                .isEqualTo(defaults.getMaxConcurrentDestinations());
        assertThat(sink.getMaxStagingFileBytes()).isEqualTo(defaults.getMaxStagingFileBytes());
        assertThat(sink.getMaxOpenDestinations()).isEqualTo(defaults.getMaxOpenDestinations());
        assertThat(RecoveryOptions.parse(arguments("--mode", "ALO")).checkpointInterval)
                .isEqualTo(RecoveryOptions.parse(arguments()).checkpointInterval)
                .isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void fileLoadsArgumentsReachTheSink() {
        var sink =
                RecoveryOptions.parse(
                                arguments(
                                        "--mode",
                                        "FILE_LOADS",
                                        "--staging-format",
                                        "PARQUET",
                                        "--max-concurrent-checkpoint-finalizations",
                                        "3",
                                        "--max-concurrent-destinations",
                                        "5",
                                        "--max-staging-file-bytes",
                                        "8388608",
                                        "--max-open-destinations",
                                        "7"))
                        .fileLoads;
        // Building PARQUET options passes the connector's probe for parquet-avro and Hadoop, on
        // the test classpath; the image's runtime classpath is the copied runtime dependencies.
        assertThat(sink.getStagingFormat()).isEqualTo(StagingFormat.PARQUET);
        assertThat(sink.getMaxConcurrentCheckpointFinalizations()).isEqualTo(3);
        assertThat(sink.getMaxConcurrentDestinations()).isEqualTo(5);
        assertThat(sink.getMaxStagingFileBytes()).isEqualTo(8388608);
        assertThat(sink.getMaxOpenDestinations()).isEqualTo(7);
    }

    @Test
    void connectorOwnsTheFileLoadsRanges() {
        assertThatThrownBy(
                        () ->
                                RecoveryOptions.parse(
                                        arguments(
                                                "--mode",
                                                "FILE_LOADS",
                                                "--max-concurrent-destinations",
                                                "65")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                RecoveryOptions.parse(
                                        arguments(
                                                "--mode",
                                                "FILE_LOADS",
                                                "--max-open-destinations",
                                                "10001")))
                .hasMessageContaining("maxPendingFiles must be >= maxOpenDestinations");
    }

    @Test
    void connectorDefaultsAreTheOnesTheCuePackageRenders() {
        // kubernetes/pkg/bigquery/application.cue writes these as #FileLoads defaults, and its
        // render tests pin them; a connector default that moves must move there as well.
        assertThat(FileLoadsOptions.DEFAULT_STAGING_FORMAT).isEqualTo(StagingFormat.AVRO);
        assertThat(FileLoadsOptions.DEFAULT_MAX_CONCURRENT_CHECKPOINT_FINALIZATIONS).isEqualTo(1);
        assertThat(FileLoadsOptions.DEFAULT_MAX_CONCURRENT_DESTINATIONS).isEqualTo(8);
        assertThat(FileLoadsOptions.DEFAULT_MAX_STAGING_FILE_BYTES).isEqualTo(16777216);
        assertThat(FileLoadsOptions.DEFAULT_MAX_OPEN_DESTINATIONS).isEqualTo(16);
        // The application does not expose maxPendingFiles, so it caps maxOpenDestinations.
        assertThat(FileLoadsOptions.DEFAULT_MAX_PENDING_FILES).isEqualTo(10000);
        assertThat(RecoveryOptions.parse(arguments()).fileLoads).isNull();
        assertThat(RecoveryOptions.parse(arguments("--mode", "ALO")).location).isNull();
        assertThat(RecoveryOptions.parse(arguments()).location).isNull();
    }

    @Test
    void testTargetReplacesTheTablesStagingAndInterval() {
        var target =
                new RecoveryOptions.Target(
                        "it-project",
                        "it_dataset",
                        "europe-west1",
                        "gs://it-bucket/it",
                        Duration.ofSeconds(2));
        var options = RecoveryOptions.parse(target, arguments("--mode", "FILE_LOADS"));
        assertThat(options.table(0).getProject()).isEqualTo("it-project");
        assertThat(options.table(0).getDataset()).isEqualTo("it_dataset");
        assertThat(options.checkpointInterval).isEqualTo(Duration.ofSeconds(2));
        assertThat(options.location).isEqualTo("europe-west1");
        var sink = options.fileLoads;
        assertThat(sink.getStagingPath()).isEqualTo("gs://it-bucket/it/bigquery-it/staging");
        assertThat(sink.getMinCheckpointInterval()).isEqualTo(Duration.ofSeconds(2));
        assertThat(RecoveryOptions.parse(arguments("--mode", "FILE_LOADS")).identity())
                .isEqualTo(options.identity());
    }

    @Test
    void rejectsDuplicateAndMissingArguments() {
        assertThatThrownBy(() -> RecoveryOptions.parse("--run-id", "a", "--run-id", "b"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RecoveryOptions.parse("--run-id"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void everyDestinationUsesAnIsolatedFixedDatasetTable() throws Exception {
        var options = RecoveryOptions.parse(arguments("--destinations", "50"));
        assertThat(IntStream.range(0, 100).mapToObj(options::table).distinct().toList())
                .hasSize(50)
                .allSatisfy(
                        table -> {
                            assertThat(table.getProject()).isEqualTo("flink-gcp");
                            assertThat(table.getDataset()).isEqualTo("flink_gcp_tier3_bigquery");
                            assertThat(table.getTable()).matches("bq_bigquery_it_d[0-9]+");
                        });
        assertThat(options.table(50)).isSameAs(options.table(0));
        var copy = InstantiationUtil.clone(options);
        assertThat(copy.table(50)).isSameAs(copy.table(0)).isEqualTo(options.table(0));
        assertThatThrownBy(() -> options.table(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> options.table(options.records))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(RecoveryOptions.parse(arguments("--run-id", "bigqueryit")).table(0))
                .isNotEqualTo(options.table(0));
    }

    @Test
    void upgradePreservesTheInputIdentity() {
        var initial = RecoveryOptions.parse(arguments());
        var upgrade =
                RecoveryOptions.parse(
                        arguments("--phase", "upgrade", "--require-restored", "true"));
        assertThat(upgrade.identity()).isEqualTo(initial.identity());
        assertThat(RecoveryOptions.parse(arguments("--records", "100")).identity())
                .isNotEqualTo(initial.identity());
    }

    @Test
    void storageWriteIdentitiesKeepTheirShape() {
        assertThat(RecoveryOptions.parse(arguments()).identity())
                .isEqualTo("v1/bigquery-it/EO/10/1843200/1048576");
        assertThat(RecoveryOptions.parse(arguments("--mode", "ALO")).identity())
                .isEqualTo("v1/bigquery-it/ALO/10/28800/1048576");
    }

    @ParameterizedTest
    @CsvSource({
        "staging-format,PARQUET",
        "max-concurrent-checkpoint-finalizations,2",
        "max-concurrent-destinations,9",
        "max-staging-file-bytes,8388608",
        "max-open-destinations,17"
    })
    void everyFileLoadsArgumentIsPartOfTheIdentity(String name, String value) {
        var initial = RecoveryOptions.parse(arguments("--mode", "FILE_LOADS"));
        assertThat(initial.identity())
                .isEqualTo("v1/bigquery-it/FILE_LOADS/10/1843200/1048576/AVRO/1/8/16777216/16");
        var upgrade =
                RecoveryOptions.parse(
                        arguments(
                                "--mode",
                                "FILE_LOADS",
                                "--phase",
                                "upgrade",
                                "--require-restored",
                                "true"));
        assertThat(upgrade.identity()).isEqualTo(initial.identity());
        assertThat(
                        RecoveryOptions.parse(arguments("--mode", "FILE_LOADS", "--" + name, value))
                                .identity())
                .isNotEqualTo(initial.identity());
    }
}
