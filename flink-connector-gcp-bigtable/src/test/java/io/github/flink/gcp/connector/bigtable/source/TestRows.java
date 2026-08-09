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

import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;

import java.util.Collections;

/**
 * Row fixtures for the source's tests.
 *
 * <p>{@code Row.create} and {@code RowCell.create} are the client library's own factories. They are
 * annotated {@code @InternalApi} while their classes are {@code @InternalExtensionOnly}, so calling
 * them is sanctioned and only extending the types is not — which is why no helper in the vendor's
 * package is needed here. A test double that had to fabricate a value with <em>no</em> reachable
 * factory would be the case that calls for one.
 */
public final class TestRows {

    /** The column family every fixture writes into. */
    public static final String FAMILY = "cf";

    private TestRows() {}

    /** Returns a row with one cell, keyed by the given UTF-8 text. */
    public static Row row(String key) {
        return row(ByteString.copyFromUtf8(key));
    }

    /** Returns a row with one cell, keyed by the given bytes. */
    public static Row row(ByteString key) {
        RowCell cell =
                RowCell.create(
                        FAMILY, ByteString.copyFromUtf8("q"), 1_000L, Collections.emptyList(), key);
        return Row.create(key, Collections.singletonList(cell));
    }

    /** Returns the row key as UTF-8 text, for readable assertions. */
    public static String keyOf(Row row) {
        return row.getKey().toStringUtf8();
    }
}
