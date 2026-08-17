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

package io.github.flink.gcp.connector.bigquery.source.enumerator;

import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplit;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The checkpointed state's identity contract.
 *
 * <p>The serializer round-trip compares states only on the all-equal side; these cases are the
 * inequality arms, one field at a time — a term {@code equals} misses is a term a restored state
 * can silently differ in while its round-trip test stays green.
 */
class BigQueryReadEnumeratorStateTest {

    private static final String SESSION = "projects/p/locations/l/sessions/s";
    private static final Instant EXPIRES = Instant.parse("2026-08-12T18:00:00Z");
    private static final List<BigQueryReadStreamSplit> SPLITS =
            Collections.singletonList(
                    new BigQueryReadStreamSplit(SESSION + "/streams/one", 0L, "{}", EXPIRES));

    @Test
    void equalStatesAgreeOnEqualsAndHashCode() {
        assertThat(state()).isEqualTo(state());
        assertThat(state().hashCode()).isEqualTo(state().hashCode());
    }

    @Test
    void comparesEveryField() {
        BigQueryReadEnumeratorState base = state();

        assertThat(new BigQueryReadEnumeratorState(false, SESSION, EXPIRES, SPLITS))
                .as("initialized")
                .isNotEqualTo(base);
        assertThat(new BigQueryReadEnumeratorState(true, SESSION + "2", EXPIRES, SPLITS))
                .as("sessionName")
                .isNotEqualTo(base);
        assertThat(new BigQueryReadEnumeratorState(true, SESSION, null, SPLITS))
                .as("sessionExpireTime")
                .isNotEqualTo(base);
        assertThat(new BigQueryReadEnumeratorState(true, SESSION, EXPIRES, Collections.emptyList()))
                .as("pendingSplits")
                .isNotEqualTo(base);
    }

    @Test
    void isNeverEqualToNullOrAnotherType() {
        assertThat(state()).isNotEqualTo(null).isNotEqualTo(SESSION);
    }

    private static BigQueryReadEnumeratorState state() {
        return new BigQueryReadEnumeratorState(true, SESSION, EXPIRES, SPLITS);
    }
}
