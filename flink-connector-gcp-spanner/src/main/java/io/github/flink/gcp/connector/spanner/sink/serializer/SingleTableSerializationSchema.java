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

package io.github.flink.gcp.connector.spanner.sink.serializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/**
 * A serializer whose every mutation names one table, which it declares so that the sink's writer
 * reads that table's schema alone rather than the whole database's.
 *
 * <p>The declaration narrows the schema the writer reads, so a serializer that names any other
 * table must not implement this: the writer would count that table's mutations without their index
 * entries, and order them against the whole batch under the per-key mode. The Table sink's
 * serializer is the one implementation.
 *
 * @param <T> type of the records serialized
 */
@Internal
public interface SingleTableSerializationSchema<T> extends SpannerMutationSerializationSchema<T> {

    /**
     * Returns the table every mutation of the serializer names.
     *
     * @return the table's native API name, {@code table} or {@code schema.table}
     */
    String table();

    /**
     * Returns the one table a serializer declares, or {@code null} when it may name any table.
     *
     * @param serializer the sink's serializer
     * @return the table's native API name, or {@code null} for every table of the database
     * @throws IllegalArgumentException if the serializer declares one table but names none, which
     *     would otherwise read as every table
     */
    @Nullable
    static String tableOf(SpannerMutationSerializationSchema<?> serializer) {
        if (!(serializer instanceof SingleTableSerializationSchema)) {
            return null;
        }
        String table = ((SingleTableSerializationSchema<?>) serializer).table();
        Preconditions.checkArgument(
                table != null,
                "A SingleTableSerializationSchema must name the table it writes; %s returned null.",
                serializer.getClass().getName());
        return table;
    }
}
