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

package io.github.flink.gcp.connector.bigtable.sink.tables;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.ColumnFamilyType;

import java.io.IOException;
import java.util.Map;

/**
 * Reads the metadata that authorizes a staged target send; injectable for deterministic failures.
 */
@Internal
@FunctionalInterface
public interface StagedTableValidator {
    /** Rejects incompatible routing, marker retention or declared data families before sending. */
    void validate(
            TableDestination destination,
            String profile,
            String markerFamily,
            Map<String, ColumnFamilyType> expectedFamilies)
            throws IOException;
}
