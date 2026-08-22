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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.annotation.PublicEvolving;

/**
 * What a fixed-width decoder does with a cell or row key longer than the declared type's layout.
 *
 * <p>A {@code BOOLEAN} cell is outside this choice: HBase's {@code Bytes.toBoolean} rejects any
 * array that is not exactly one byte, and the decoder mirrors that under both values.
 */
@PublicEvolving
public enum TrailingBytes {

    /**
     * Decode the declared width and ignore the rest, which is what HBase's {@code Bytes} decoders
     * do. This reads the leading component of a composite key, and it is what makes an equality
     * predicate on a fixed-width row key match every key that decodes to the compared value.
     */
    IGNORE("ignore"),

    /**
     * Fail the read on any length but the declared type's exact width, so a value that was not
     * written under this connector's encoding cannot silently decode as its prefix.
     */
    REJECT("reject");

    private final String value;

    TrailingBytes(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
