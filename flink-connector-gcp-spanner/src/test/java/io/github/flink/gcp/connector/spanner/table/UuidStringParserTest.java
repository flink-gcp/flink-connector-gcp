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

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UuidStringParserTest {

    @Test
    void acceptsEitherCase() {
        UUID uuid = UuidStringParser.parse("F81D4FAE-7DEC-11D0-A765-00A0C91E6BF6", "id");

        assertThat(uuid).isEqualTo(UUID.fromString("f81d4fae-7dec-11d0-a765-00a0c91e6bf6"));
    }

    @Test
    void rejectsMalformedAndJavaShortenedFormsWithoutEchoingTheValue() {
        for (String invalid : new String[] {"not-a-uuid", "1-1-1-1-1"}) {
            assertThatThrownBy(() -> UuidStringParser.parse(invalid, "external_id"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("column 'external_id'")
                    .hasMessageContaining("8-4-4-4-12")
                    .hasMessageNotContaining(invalid);
        }
    }
}
