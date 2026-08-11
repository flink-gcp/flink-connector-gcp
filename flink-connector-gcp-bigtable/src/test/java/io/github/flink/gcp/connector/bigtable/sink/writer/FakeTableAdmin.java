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

package io.github.flink.gcp.connector.bigtable.sink.writer;

import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigtable.sink.tables.TableAdmin;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Set;

/**
 * A {@link TableAdmin} for writer tests: records what was ensured, answers with a scripted result
 * or failure, and runs an optional hook — typically one that clears the fake batcher's
 * missing-table state, so repair convergence emerges from the ensure the way it does against the
 * service rather than being scripted turn by turn.
 */
final class FakeTableAdmin implements TableAdmin {

    final List<TableDestination> ensured = new ArrayList<>();
    final List<TableCreateOptions> ensureOptions = new ArrayList<>();

    /** Thrown by successive {@code ensureTable} calls, one per call, until empty. */
    final Deque<IOException> ensureFailures = new ArrayDeque<>();

    /** What {@code ensureTable} reports; defaults to "created the table". */
    EnsureResult result = EnsureResult.created(Set.of("cf"));

    /** Runs after a successful ensure, before the result is returned. */
    @Nullable Runnable onEnsure;

    int closeCalls;

    @Override
    public EnsureResult ensureTable(TableDestination destination, TableCreateOptions options)
            throws IOException {
        ensured.add(destination);
        ensureOptions.add(options);
        IOException failure = ensureFailures.poll();
        if (failure != null) {
            throw failure;
        }
        if (onEnsure != null) {
            onEnsure.run();
        }
        return result;
    }

    @Override
    public void close() {
        closeCalls++;
    }
}
