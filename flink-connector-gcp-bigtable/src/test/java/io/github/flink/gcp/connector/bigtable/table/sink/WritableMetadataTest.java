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

package io.github.flink.gcp.connector.bigtable.table.sink;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Where a metadata column is, and which constant a key names.
 *
 * <p>The sink's own tests reach both through a mutation, which cannot separate "the position was
 * read out of the selection" from "the position happens to be the first column after the physical
 * ones" while the enum has one constant. These do.
 */
class WritableMetadataTest {

    private static final WritableMetadata TIMESTAMP = WritableMetadata.TIMESTAMP;

    @Test
    void theTimestampColumnIsWhereTheSelectionPutIt() {
        // The hole stands in for the second constant this enum does not have yet: the planner
        // never hands back a null, but it does hand back a list this one is not first in, and that
        // is the case an index derived as "the first column after the physical ones" gets wrong.
        // Whoever adds a constant should put it here instead of the null.
        assertThat(TIMESTAMP.position(3, new WritableMetadata[] {null, TIMESTAMP})).isEqualTo(4);
        // Both ends, because a selection of length one cannot tell "this constant's column" from
        // "the first" or "the last" — an implementation reading neither end still has to be wrong
        // in one of these two.
        assertThat(TIMESTAMP.position(3, new WritableMetadata[] {TIMESTAMP, null})).isEqualTo(3);
    }

    @Test
    void metadataTheDdlDidNotDeclareHasNoColumn() {
        assertThat(TIMESTAMP.position(3, new WritableMetadata[0])).isEqualTo(-1);
        assertThat(TIMESTAMP.position(3, new WritableMetadata[] {null})).isEqualTo(-1);
    }

    @Test
    void everyOfferedKeyNamesTheConstantThatOfferedIt() {
        for (WritableMetadata metadata : WritableMetadata.values()) {
            assertThat(WritableMetadata.of(metadata.getKey()))
                    .as("key '%s'", metadata.getKey())
                    .isSameAs(metadata);
        }
    }

    @Test
    void anUnknownKeyIsNamedRatherThanIgnored() {
        assertThatThrownBy(() -> WritableMetadata.of("cell-visibility"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unknown Bigtable writable metadata key 'cell-visibility'.");
    }
}
