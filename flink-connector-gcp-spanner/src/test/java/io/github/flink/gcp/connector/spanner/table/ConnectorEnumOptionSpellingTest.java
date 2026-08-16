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

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/** Pins the DDL spellings carried explicitly by the Spanner table connector's enums. */
class ConnectorEnumOptionSpellingTest {

    @Test
    void enumsSpellThemselvesTheWayADdlIsWritten() {
        assertThat(ScanMode.BOUNDED).hasToString("bounded");
        assertThat(ScanMode.CHANGE_STREAM).hasToString("change-stream");

        assertThat(ChangeStreamChangelogMode.FULL).hasToString("full");
        assertThat(ChangeStreamChangelogMode.UPSERT).hasToString("upsert");

        assertThat(ChangeStreamStartMode.EARLIEST).hasToString("earliest");
        assertThat(ChangeStreamStartMode.LATEST).hasToString("latest");
        assertThat(ChangeStreamStartMode.TIMESTAMP).hasToString("timestamp");
    }

    @Test
    void everyConstantParsesBackFromItsOwnSpelling() {
        assertRoundTrips(ScanMode.class);
        assertRoundTrips(ChangeStreamChangelogMode.class);
        assertRoundTrips(ChangeStreamStartMode.class);
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
