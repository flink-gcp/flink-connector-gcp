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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.writer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;

/** Lifecycle tests for {@link GcsStagingStorage}. */
class GcsStagingStorageTest {

    @Test
    void closingAnInstanceThatNeverBuiltAClientDoesNothing() {
        // Every subtask deserializes an instance whose client is built on first use, and a
        // committer that commits nothing never asks for one. Closing must therefore tolerate the
        // client being absent — Flink calls close() on the writer and the committer either way.
        //
        // This is the whole of what a unit test can say about this close(): the client itself is a
        // ~100-method SDK interface with no injection seam here, so closing a real one is only
        // observable against the service.
        assertThatCode(new GcsStagingStorage()::close).doesNotThrowAnyException();
    }
}
