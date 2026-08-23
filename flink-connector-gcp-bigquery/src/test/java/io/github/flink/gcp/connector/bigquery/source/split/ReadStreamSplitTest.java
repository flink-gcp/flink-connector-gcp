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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The split's identity and construction contract.
 *
 * <p>The serializer round-trips compare splits only on the all-equal side; these cases are the
 * inequality arms, one field at a time, because a term {@code equals} misses is a term a restored
 * split can silently differ in while comparing equal.
 */
class ReadStreamSplitTest {

    private static final String STREAM = "projects/p/locations/l/sessions/s/streams/one";
    private static final Instant EXPIRES = Instant.parse("2026-08-12T18:00:00Z");

    @Test
    void equalSplitsAgreeOnEqualsAndHashCode() {
        assertThat(split()).isEqualTo(split());
        assertThat(split().hashCode()).isEqualTo(split().hashCode());
    }

    @Test
    void comparesEveryField() {
        ReadStreamSplit base = split();

        assertThat(new ReadStreamSplit(STREAM + "2", 4L, "{}", EXPIRES))
                .as("streamName")
                .isNotEqualTo(base);
        assertThat(new ReadStreamSplit(STREAM, 5L, "{}", EXPIRES)).as("offset").isNotEqualTo(base);
        assertThat(new ReadStreamSplit(STREAM, 4L, "{\"a\":1}", EXPIRES))
                .as("avroSchemaJson")
                .isNotEqualTo(base);
        assertThat(new ReadStreamSplit(STREAM, 4L, "{}", null))
                .as("sessionExpireTime")
                .isNotEqualTo(base);
    }

    @Test
    void isNeverEqualToNullOrAnotherType() {
        assertThat(split()).isNotEqualTo(null).isNotEqualTo(STREAM);
    }

    @Test
    void rejectsANegativeOffset() {
        // The offset counts rows already consumed, so a negative one can only be a bookkeeping
        // bug; failing at construction names where it happened rather than where it was read.
        assertThatThrownBy(() -> new ReadStreamSplit(STREAM, -1L, "{}", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("offset must not be negative");
    }

    private static ReadStreamSplit split() {
        return new ReadStreamSplit(STREAM, 4L, "{}", EXPIRES);
    }
}
