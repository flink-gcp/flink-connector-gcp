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

package io.github.flink.gcp.connector.spanner.source;

import com.google.cloud.spanner.Struct;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds the rows the source tests read.
 *
 * <p>Unlike a partition, a {@link Struct} has a public builder, so nothing here needs to reach into
 * the vendor's package.
 */
public final class TestStructs {

    private TestStructs() {}

    /**
     * Returns a row with an {@code id} column.
     *
     * @param id the value of the column
     * @return the row
     */
    public static Struct row(long id) {
        return Struct.newBuilder().set("id").to(id).build();
    }

    /**
     * Returns rows with an {@code id} column, one per value.
     *
     * @param ids the values
     * @return the rows
     */
    public static List<Struct> rows(long... ids) {
        List<Struct> rows = new ArrayList<>(ids.length);
        for (long id : ids) {
            rows.add(row(id));
        }
        return rows;
    }

    /**
     * Returns the {@code id} column of a row.
     *
     * @param row the row
     * @return the value of the column
     */
    public static long idOf(Struct row) {
        return row.getLong("id");
    }
}
