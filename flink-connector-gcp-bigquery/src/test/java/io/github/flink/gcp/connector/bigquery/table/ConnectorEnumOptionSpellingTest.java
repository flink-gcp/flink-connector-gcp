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

package io.github.flink.gcp.connector.bigquery.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import com.google.cloud.bigquery.TimePartitioning;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.sink.WriteDisposition;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.ParquetCompression;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the DDL spelling of the connector enums a {@code ConfigOption} resolves.
 *
 * <p>Flink's {@code ConfigurationUtils.convertToEnum} matches on {@code toString()},
 * case-insensitively, and normalizes nothing else — so these enums carry their DDL spelling in
 * {@code toString()} and this test lives beside the table layer, which is the only reason the
 * spelling is what it is.
 */
class ConnectorEnumOptionSpellingTest {

    @Test
    void enumsSpellThemselvesTheWayADdlIsWritten() {
        assertSpellings(
                WriteMethod.class,
                "storage-api-at-least-once",
                "storage-api-exactly-once",
                "file-loads");
        assertSpellings(CreateDisposition.class, "create-if-needed", "create-never");
        assertSpellings(
                WriteDisposition.class,
                "write-append",
                "write-truncate",
                "write-truncate-data",
                "write-empty");
        assertSpellings(
                TableCreateOptions.TimePartitioningType.class, "hour", "day", "month", "year");
        assertSpellings(StagingFormat.class, "avro", "parquet");
        assertSpellings(ParquetCompression.class, "zstd", "none");
    }

    @Test
    void everyConstantOfEveryEnumParsesBackFromItsOwnSpelling() {
        assertRoundTrips(WriteMethod.class);
        assertRoundTrips(CreateDisposition.class);
        assertRoundTrips(WriteDisposition.class);
        assertRoundTrips(TableCreateOptions.TimePartitioningType.class);
        assertRoundTrips(StagingFormat.class);
        assertRoundTrips(ParquetCompression.class);
    }

    @Test
    void thePartitioningTypeStillNamesTheClientLibrarysOwnConstant() {
        // BigQueryTableAdmin bridges with TimePartitioning.Type.valueOf(type.name()), so the
        // constant names are the contract with the client library and the DDL spelling is not.
        for (TableCreateOptions.TimePartitioningType type :
                TableCreateOptions.TimePartitioningType.values()) {
            assertThat(TimePartitioning.Type.valueOf(type.name()))
                    .as("%s", type.name())
                    .isNotNull();
        }
    }

    @Test
    void aMessageMeaningTheJavaConstantStillSaysTheJavaConstant() {
        // The builder's write-method messages name WriteMethod.FILE_LOADS, so the value they format
        // beside it must be the constant too, not the DDL spelling.
        assertThat(WriteMethod.FILE_LOADS.name()).isEqualTo("FILE_LOADS");
        // Same for the streaming write-disposition message in BigQueryFileLoadsSink, which names
        // WRITE_APPEND and the non-append constants in its prose. The message itself is pinned by
        // BigQueryFileLoadsSinkTopologyTest; this is the spelling that makes it possible.
        assertThat(WriteDisposition.WRITE_TRUNCATE.name()).isEqualTo("WRITE_TRUNCATE");
        assertThat(WriteDisposition.WRITE_TRUNCATE_DATA.name()).isEqualTo("WRITE_TRUNCATE_DATA");
    }

    private static <E extends Enum<E>> void assertRoundTrips(Class<E> enumClass) {
        ConfigOption<E> option = ConfigOptions.key("k").enumType(enumClass).noDefaultValue();
        for (E constant : enumClass.getEnumConstants()) {
            assertThat(parse(option, constant.toString())).isEqualTo(constant);
            assertThat(parse(option, constant.toString().toUpperCase(Locale.ROOT)))
                    .isEqualTo(constant);
        }
    }

    private static <E extends Enum<E>> void assertSpellings(
            Class<E> enumClass, String... expectedSpellings) {
        assertThat(enumClass.getEnumConstants())
                .extracting(Object::toString)
                .containsExactly(expectedSpellings);
    }

    private static <E extends Enum<E>> E parse(ConfigOption<E> option, String value) {
        return Configuration.fromMap(Collections.singletonMap(option.key(), value)).get(option);
    }
}
