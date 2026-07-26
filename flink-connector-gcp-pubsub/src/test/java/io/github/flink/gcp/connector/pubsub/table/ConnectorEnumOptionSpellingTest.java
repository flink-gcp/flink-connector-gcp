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

package io.github.flink.gcp.connector.pubsub.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import io.github.flink.gcp.connector.pubsub.sink.CreateDisposition;
import io.github.flink.gcp.connector.pubsub.source.DeserializationFailurePolicy;
import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.StartPosition;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The DDL spelling of the connector's enums.
 *
 * <p>Flink resolves an enum {@code ConfigOption} by matching the configured value against {@code
 * toString()} — case-insensitively, but with no other normalization, so an underscore in {@code
 * toString()} is an underscore in the DDL. These enums therefore carry their option spelling, and
 * this test is what keeps that true: it lives beside the table layer because that is the only
 * reason the spelling is what it is.
 */
class ConnectorEnumOptionSpellingTest {

    @Test
    void enumsSpellThemselvesTheWayADdlIsWritten() {
        assertThat(CreateDisposition.CREATE_IF_NEEDED).hasToString("create-if-needed");
        assertThat(CreateDisposition.CREATE_NEVER).hasToString("create-never");

        assertThat(OrderingMode.NONE).hasToString("none");
        assertThat(OrderingMode.PER_KEY).hasToString("per-key");

        assertThat(DeserializationFailurePolicy.FAIL).hasToString("fail");
        assertThat(DeserializationFailurePolicy.DROP).hasToString("drop");
        assertThat(DeserializationFailurePolicy.NACK).hasToString("nack");

        assertThat(StartPosition.Mode.CONTINUE_FROM_SUBSCRIPTION)
                .hasToString("continue-from-subscription");
        assertThat(StartPosition.Mode.EARLIEST_RETAINED).hasToString("earliest-retained");
        assertThat(StartPosition.Mode.LATEST).hasToString("latest");
        assertThat(StartPosition.Mode.TIMESTAMP).hasToString("timestamp");
    }

    @Test
    void everyConstantOfEveryEnumParsesBackFromItsOwnSpelling() {
        assertRoundTrips(CreateDisposition.class);
        assertRoundTrips(OrderingMode.class);
        assertRoundTrips(DeserializationFailurePolicy.class);
        assertRoundTrips(StartPosition.Mode.class);
    }

    /**
     * Feeds each constant's {@code toString()} back through a {@code ConfigOption} of its type, the
     * way the factory reads a DDL value, and in upper case as well — a user may type either, and
     * Flink's matching is case-insensitive.
     */
    private static <E extends Enum<E>> void assertRoundTrips(Class<E> enumClass) {
        ConfigOption<E> option = ConfigOptions.key("k").enumType(enumClass).noDefaultValue();
        for (E constant : enumClass.getEnumConstants()) {
            assertThat(parse(option, constant.toString()))
                    .as("%s spelled as written", constant.name())
                    .isEqualTo(constant);
            assertThat(parse(option, constant.toString().toUpperCase(Locale.ROOT)))
                    .as("%s spelled in upper case", constant.name())
                    .isEqualTo(constant);
        }
    }

    private static <E extends Enum<E>> E parse(ConfigOption<E> option, String value) {
        return Configuration.fromMap(Collections.singletonMap(option.key(), value)).get(option);
    }
}
