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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.Collections;
import java.util.Map;

/** The non-cell-value field a Bigtable table row may supply to its mutation. */
@Internal
enum WritableMetadata {

    /** One timestamp applied to every cell the row writes; {@code null} keeps the writer clock. */
    TIMESTAMP("timestamp", DataTypes.TIMESTAMP_LTZ(6).nullable());

    private final String key;
    private final DataType dataType;

    WritableMetadata(String key, DataType dataType) {
        this.key = key;
        this.dataType = dataType;
    }

    String getKey() {
        return key;
    }

    /** Returns the metadata this connector can write. */
    static Map<String, DataType> listAll() {
        return Collections.singletonMap(TIMESTAMP.key, TIMESTAMP.dataType);
    }
}
