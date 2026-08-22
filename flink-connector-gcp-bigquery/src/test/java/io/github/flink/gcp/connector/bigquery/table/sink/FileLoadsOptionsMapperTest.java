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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.bigquery.sink.WriteDisposition;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.ParquetCompression;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
import io.github.flink.gcp.connector.bigquery.table.BigQueryConnectorOptions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FileLoadsOptionsMapper}. */
class FileLoadsOptionsMapperTest {

    /** The key prefix the whole family shares, for the reflective coverage test below. */
    private static final String PREFIX = "sink.file-loads.";

    /**
     * Every {@code FileLoadsOptions.Builder} setter and the option that feeds it.
     *
     * <p>Keyed by <b>setter</b>, which is what a mapper calls. The keys are spelled after the
     * {@code schemaReconcile*} vocabulary, which keeps them clear of the unrelated {@code
     * sink.schema-update.*} family.
     */
    private static final Map<String, ConfigOption<?>> SETTER_TO_OPTION = new LinkedHashMap<>();

    static {
        SETTER_TO_OPTION.put("stagingPath", BigQueryConnectorOptions.SINK_FILE_LOADS_STAGING_PATH);
        SETTER_TO_OPTION.put("tempDataset", BigQueryConnectorOptions.SINK_FILE_LOADS_TEMP_DATASET);
        SETTER_TO_OPTION.put(
                "writeDisposition", BigQueryConnectorOptions.SINK_FILE_LOADS_WRITE_DISPOSITION);
        SETTER_TO_OPTION.put(
                "minCheckpointInterval",
                BigQueryConnectorOptions.SINK_FILE_LOADS_MIN_CHECKPOINT_INTERVAL);
        SETTER_TO_OPTION.put(
                "maxStagingFileBytes",
                BigQueryConnectorOptions.SINK_FILE_LOADS_MAX_STAGING_FILE_BYTES);
        SETTER_TO_OPTION.put(
                "stagingFormat", BigQueryConnectorOptions.SINK_FILE_LOADS_STAGING_FORMAT);
        SETTER_TO_OPTION.put(
                "parquetCompression", BigQueryConnectorOptions.SINK_FILE_LOADS_PARQUET_COMPRESSION);
        SETTER_TO_OPTION.put(
                "loadJobPollInitialBackoff",
                BigQueryConnectorOptions.SINK_FILE_LOADS_LOAD_JOB_POLL_INITIAL_BACKOFF);
        SETTER_TO_OPTION.put(
                "loadJobPollMaxBackoff",
                BigQueryConnectorOptions.SINK_FILE_LOADS_LOAD_JOB_POLL_MAX_BACKOFF);
        SETTER_TO_OPTION.put(
                "schemaReconcileInitialBackoff",
                BigQueryConnectorOptions.SINK_FILE_LOADS_SCHEMA_RECONCILE_INITIAL_BACKOFF);
        SETTER_TO_OPTION.put(
                "schemaReconcileMaxBackoff",
                BigQueryConnectorOptions.SINK_FILE_LOADS_SCHEMA_RECONCILE_MAX_BACKOFF);
        SETTER_TO_OPTION.put(
                "schemaReconcileMaxAttempts",
                BigQueryConnectorOptions.SINK_FILE_LOADS_SCHEMA_RECONCILE_MAX_ATTEMPTS);
        SETTER_TO_OPTION.put(
                "perDestinationMetrics",
                BigQueryConnectorOptions.SINK_FILE_LOADS_PER_DESTINATION_METRICS);
    }

    private static String key(String setter) {
        return SETTER_TO_OPTION.get(setter).key();
    }

    private static FileLoadsOptions map(Map<String, String> options) {
        return FileLoadsOptionsMapper.map(Configuration.fromMap(options));
    }

    /** The staging path alone, which is the least a FILE_LOADS table can configure. */
    private static Map<String, String> staged() {
        Map<String, String> options = new HashMap<>();
        options.put(key("stagingPath"), "gs://bucket/prefix");
        return options;
    }

    @Test
    void everyFileLoadsKnobHasAnOption() {
        // Not filtered on arity or name: a knob of any shape must appear, which is the whole point
        // of this guard.
        Set<String> setters =
                Arrays.stream(FileLoadsOptions.Builder.class.getDeclaredMethods())
                        .filter(m -> Modifier.isPublic(m.getModifiers()))
                        .filter(m -> m.getReturnType() == FileLoadsOptions.Builder.class)
                        .map(Method::getName)
                        .collect(Collectors.toSet());

        // Both directions: a new knob without an option, and an option whose knob was removed.
        assertThat(setters).isEqualTo(SETTER_TO_OPTION.keySet());
    }

    @Test
    void everyOptionOfTheFamilyFeedsAKnob() {
        // The other half of the guard above, and the one a new key would otherwise slip past: an
        // option declared under the prefix that no setter consumes. The expected side is read out
        // of BigQueryConnectorOptions rather than written here — a literal list would only restate
        // SETTER_TO_OPTION and could never disagree with it.
        Set<String> declared = OptionFamilies.declaredKeysUnder(PREFIX);
        // Guards the reflection itself: an empty set would make the assertion vacuous.
        assertThat(declared).isNotEmpty();

        Set<String> mapped =
                SETTER_TO_OPTION.values().stream()
                        .map(ConfigOption::key)
                        .collect(Collectors.toSet());

        assertThat(mapped).isEqualTo(declared);
    }

    @Test
    void aMissingStagingPathIsReportedInOptionKeys() {
        // The one rule this mapper owns. FileLoadsOptions.build() rejects it too, naming
        // stagingPath("gs://...") — a builder method a SQL user cannot call.
        assertThatThrownBy(() -> map(new HashMap<>()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(key("stagingPath"))
                .hasMessageContaining("no default location to stage them in");
    }

    @Test
    void theStagingPathAloneLeavesEveryOtherKnobAtItsDefault() {
        // Unlike DefaultStreamOptionsMapper, which returns null when its family is untouched: the
        // builder requires this object for FILE_LOADS, so a DDL that names only the staging path
        // must still get one.
        assertThat(map(staged()))
                .isEqualTo(FileLoadsOptions.builder().stagingPath("gs://bucket/prefix").build());
    }

    @Test
    void mapsEveryOptionOntoItsKnob() {
        Map<String, String> options = staged();
        options.put(key("tempDataset"), "staging_dataset");
        options.put(key("writeDisposition"), "write-truncate-data");
        options.put(key("minCheckpointInterval"), "30 s");
        // A MemorySize key, so the unit suffix is the point: a plain "64" would also parse, and
        // would pass whether or not the mapper converted the value.
        options.put(key("maxStagingFileBytes"), "64 mb");
        options.put(key("stagingFormat"), "parquet");
        options.put(key("parquetCompression"), "none");
        options.put(key("loadJobPollInitialBackoff"), "2 s");
        options.put(key("loadJobPollMaxBackoff"), "40 s");
        options.put(key("schemaReconcileInitialBackoff"), "1 s");
        options.put(key("schemaReconcileMaxBackoff"), "20 s");
        options.put(key("schemaReconcileMaxAttempts"), "3");
        options.put(key("perDestinationMetrics"), "true");

        FileLoadsOptions mapped = map(options);

        assertThat(mapped.getStagingPath()).isEqualTo("gs://bucket/prefix");
        assertThat(mapped.getTempDataset()).isEqualTo("staging_dataset");
        assertThat(mapped.getWriteDisposition()).isEqualTo(WriteDisposition.WRITE_TRUNCATE_DATA);
        assertThat(mapped.getMinCheckpointInterval()).isEqualTo(Duration.ofSeconds(30));
        assertThat(mapped.getMaxStagingFileBytes()).isEqualTo(64L * 1024 * 1024);
        assertThat(mapped.getStagingFormat()).isEqualTo(StagingFormat.PARQUET);
        assertThat(mapped.getParquetCompression()).isEqualTo(ParquetCompression.NONE);
        assertThat(mapped.getLoadJobPollInitialBackoff()).isEqualTo(Duration.ofSeconds(2));
        assertThat(mapped.getLoadJobPollMaxBackoff()).isEqualTo(Duration.ofSeconds(40));
        assertThat(mapped.getSchemaReconcileInitialBackoff()).isEqualTo(Duration.ofSeconds(1));
        assertThat(mapped.getSchemaReconcileMaxBackoff()).isEqualTo(Duration.ofSeconds(20));
        assertThat(mapped.getSchemaReconcileMaxAttempts()).isEqualTo(3);
        assertThat(mapped.isPerDestinationMetrics()).isTrue();
    }

    @Test
    void parquetCompressionUnderAvroIsRejectedRatherThanIgnored() {
        // The mapper applies it unconditionally so the builder's rule fires. A DDL that sets a
        // Parquet codec on an Avro table has said something contradictory, and silently dropping
        // it is the outcome this repository treats as worse than an error.
        Map<String, String> options = staged();
        options.put(key("parquetCompression"), "none");

        assertThatThrownBy(() -> map(options))
                .hasMessageContaining("parquetCompression")
                .hasMessageContaining("PARQUET");
    }

    @Test
    void aValueTheBuilderRejectsIsRenamedToItsOptionKey() {
        // The bound is the builder's; the mapper renames its rejection to the key the SQL caller
        // wrote and keeps the builder's sentence as the detail (#1030).
        Map<String, String> options = new HashMap<>();
        options.put(key("stagingPath"), "s3://bucket/prefix");
        assertThatThrownBy(() -> map(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Option 'sink.file-loads.staging-path' is invalid")
                .hasMessageContaining("stagingPath must be of the form gs://bucket[/prefix]");
    }

    @Test
    void reportsWhichKeysOfTheFamilyAreSet() {
        Map<String, String> options = staged();
        options.put(key("schemaReconcileMaxAttempts"), "3");

        assertThat(FileLoadsOptionsMapper.presentKeys(Configuration.fromMap(options)))
                .containsExactly(key("stagingPath"), key("schemaReconcileMaxAttempts"));
    }

    @Test
    void namesTheOptionKeyWhenAValueIsRejected() {
        Map<String, String> options = new HashMap<>();
        options.put("sink.file-loads.staging-path", "gs://bucket/prefix");
        options.put("sink.file-loads.max-staging-file-bytes", "0");

        assertThatThrownBy(() -> map(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Option 'sink.file-loads.max-staging-file-bytes' is invalid")
                .hasMessageContaining("maxStagingFileBytes must be positive");
    }
}
