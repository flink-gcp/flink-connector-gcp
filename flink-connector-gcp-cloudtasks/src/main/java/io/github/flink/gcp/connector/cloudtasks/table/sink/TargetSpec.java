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

package io.github.flink.gcp.connector.cloudtasks.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Map;

/** Validated fixed target values used by the table sink. */
@Internal
public abstract class TargetSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    abstract Map<String, DataType> writableMetadata();

    abstract WritableMetadata addressMetadata();

    abstract String addressOptionKey();

    @Nullable
    abstract String fixedAddress();

    abstract RowDataToTaskConverter converter(int physicalArity, WritableMetadata[] metadata);

    static boolean sameContentType(String expected, String actual) {
        return expected.equalsIgnoreCase(actual.trim());
    }
}
