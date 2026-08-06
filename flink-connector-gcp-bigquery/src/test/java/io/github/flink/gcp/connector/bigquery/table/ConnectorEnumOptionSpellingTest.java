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

package io.github.flink.gcp.connector.bigquery.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
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
        assertThat(WriteMethod.STORAGE_API_AT_LEAST_ONCE).hasToString("storage-api-at-least-once");
        assertThat(WriteMethod.STORAGE_API_EXACTLY_ONCE).hasToString("storage-api-exactly-once");
        assertThat(WriteMethod.FILE_LOADS).hasToString("file-loads");
        assertThat(CreateDisposition.CREATE_IF_NEEDED).hasToString("create-if-needed");
        assertThat(CreateDisposition.CREATE_NEVER).hasToString("create-never");
    }

    @Test
    void noConstantOfAnyEnumKeepsTheUnderscoreSpelling() {
        assertHyphenated(WriteMethod.class);
        assertHyphenated(CreateDisposition.class);
    }

    @Test
    void everyConstantOfEveryEnumParsesBackFromItsOwnSpelling() {
        assertRoundTrips(WriteMethod.class);
        assertRoundTrips(CreateDisposition.class);
    }

    @Test
    void aMessageMeaningTheJavaConstantStillSaysTheJavaConstant() {
        // The builder's write-method messages name WriteMethod.FILE_LOADS, so the value they format
        // beside it must be the constant too, not the DDL spelling.
        assertThat(WriteMethod.FILE_LOADS.name()).isEqualTo("FILE_LOADS");
    }

    private static <E extends Enum<E>> void assertHyphenated(Class<E> enumClass) {
        for (E constant : enumClass.getEnumConstants()) {
            assertThat(constant.toString())
                    .as("%s.%s", enumClass.getSimpleName(), constant.name())
                    .isEqualTo(constant.name().toLowerCase(Locale.ROOT).replace('_', '-'));
        }
    }

    private static <E extends Enum<E>> void assertRoundTrips(Class<E> enumClass) {
        ConfigOption<E> option = ConfigOptions.key("k").enumType(enumClass).noDefaultValue();
        for (E constant : enumClass.getEnumConstants()) {
            assertThat(parse(option, constant.toString())).isEqualTo(constant);
            assertThat(parse(option, constant.toString().toUpperCase(Locale.ROOT)))
                    .isEqualTo(constant);
        }
    }

    private static <E extends Enum<E>> E parse(ConfigOption<E> option, String value) {
        return Configuration.fromMap(Collections.singletonMap(option.key(), value)).get(option);
    }
}
