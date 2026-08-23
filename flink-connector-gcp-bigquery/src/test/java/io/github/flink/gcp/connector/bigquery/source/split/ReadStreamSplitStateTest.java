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

package io.github.flink.gcp.connector.bigquery.source.split;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ReadStreamSplitState}. */
class ReadStreamSplitStateTest {

    private static final String STREAM = "projects/p/locations/l/sessions/s/streams/one";
    private static final String SCHEMA =
            "{\"type\":\"record\",\"name\":\"Row\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}";
    private static final Instant EXPIRE_TIME = Instant.parse("2026-08-09T18:00:00Z");

    @Test
    void carriesEverythingButTheOffsetBackIntoTheCheckpointedSplit() {
        // The offset is the only thing this type is allowed to change. Anything else it dropped
        // would survive the first read and disappear at the first checkpoint, which is the point at
        // which nothing is looking: the schema would fail the next reader outright, and the expiry
        // would quietly stop explaining an expired-session failure.
        ReadStreamSplitState state =
                new ReadStreamSplitState(new ReadStreamSplit(STREAM, 7, SCHEMA, EXPIRE_TIME));

        state.recordConsumed();

        assertThat(state.toSplit()).isEqualTo(new ReadStreamSplit(STREAM, 8, SCHEMA, EXPIRE_TIME));
    }

    @Test
    void keepsAMissingExpiryMissing() {
        ReadStreamSplitState state =
                new ReadStreamSplitState(new ReadStreamSplit(STREAM, 0, SCHEMA, null));

        assertThat(state.toSplit().getSessionExpireTime()).isNull();
    }
}
