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

package io.github.flink.gcp.connector.bigtable.source;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Reads rows from a Cloud Bigtable table.
 *
 * <pre>{@code
 * Source<Order, ?, ?> source =
 *         BigtableSource.<Order>builder()
 *                 .table(TableDestination.of("my-project", "my-instance", "orders"))
 *                 .deserializer(myDeserializer)
 *                 .prefix("2026-08-")
 *                 .build();
 * }</pre>
 *
 * <p>The scan is bounded: the source reads the configured ranges and finishes. That is not the same
 * as batch-only — a bounded source runs inside a streaming pipeline and simply ends, which is what
 * makes reading a Bigtable table and joining it against an unbounded stream work.
 */
@PublicEvolving
public final class BigtableSource {

    private BigtableSource() {}

    /**
     * Returns a builder for a Bigtable scan source.
     *
     * @param <T> the record type produced
     * @return the builder
     */
    public static <T> BigtableSourceBuilder<T> builder() {
        return new BigtableSourceBuilder<>();
    }
}
