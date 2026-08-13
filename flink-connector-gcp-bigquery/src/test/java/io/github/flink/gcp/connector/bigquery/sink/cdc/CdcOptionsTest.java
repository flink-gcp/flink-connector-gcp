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

package io.github.flink.gcp.connector.bigquery.sink.cdc;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CdcOptionsTest {

    @Test
    void requiresAChangeTypeProvider() {
        assertThatThrownBy(() -> CdcOptions.builder(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("changeTypeProvider must not be null");
    }

    @Test
    void sequenceProviderIsOptionalAndRejectsNull() {
        CdcOptions<String> options =
                CdcOptions.<String>builder(CdcChangeTypeProvider.upsertOnly()).build();

        assertThat(options.getChangeTypeProvider().getChangeType("row"))
                .isEqualTo(CdcChangeType.UPSERT);
        assertThat(options.hasSequenceNumberProvider()).isFalse();
        assertThat(options.getSequenceNumberProvider()).isNull();
        assertThatThrownBy(
                        () ->
                                CdcOptions.<String>builder(CdcChangeTypeProvider.upsertOnly())
                                        .sequenceNumberProvider(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("sequenceNumberProvider must not be null");
    }
}
