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

package io.github.flink.gcp.connector.bigtable.sink.writer;

import org.apache.flink.util.ExceptionUtils;

import io.github.flink.gcp.connector.bigtable.TableDestination;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link MutationBatcherFactory} handing out one {@link FakeMutationBatcher} per table.
 *
 * <p>Every batcher of one factory shares an {@link FakeMutationBatcher#events} list, so a test can
 * read the teardown order across tables — which is the only way to see that the writer starts every
 * shutdown before it waits on any close.
 *
 * <p>{@link #batcherFor} is what a test scripts against, and it is deliberately <em>not</em> what
 * records a creation: it registers the batcher the writer will be handed when it first routes a
 * record to that table, while {@link #created} records the tables the writer actually opened. So a
 * test can both script a table's failures up front and assert the writer never opened it.
 */
final class FakeMutationBatcherFactory implements MutationBatcherFactory {

    private static final long serialVersionUID = 1L;

    /** The batchers, in registration order. */
    final Map<TableDestination, FakeMutationBatcher> batchers = new LinkedHashMap<>();

    /** The tables the writer asked for a batcher for, in call order, repeats included. */
    final List<TableDestination> created = new ArrayList<>();

    /** Teardown events across every batcher, plus this factory's own close. */
    final List<String> events = new ArrayList<>();

    /** Tables whose closed batchers the writer released, in call order. */
    final List<TableDestination> released = new ArrayList<>();

    /** Failures the next {@link #create} calls throw, one each, until empty. */
    final Deque<IOException> createFailures = new ArrayDeque<>();

    int closeCalls;
    Throwable releaseFailure;
    Throwable closeFailure;

    /**
     * Registers (or returns) the batcher for a table, so a test can script it before it is used.
     */
    FakeMutationBatcher batcherFor(TableDestination destination) {
        return batchers.computeIfAbsent(
                destination, table -> new FakeMutationBatcher(table, events));
    }

    @Override
    public MutationBatcher create(TableDestination destination) throws IOException {
        IOException failure = createFailures.poll();
        if (failure != null) {
            throw failure;
        }
        created.add(destination);
        return batcherFor(destination);
    }

    @Override
    public void release(TableDestination destination) throws Exception {
        released.add(destination);
        events.add("release:" + destination);
        if (releaseFailure != null) {
            ExceptionUtils.rethrowException(releaseFailure);
        }
    }

    @Override
    public void close() throws Exception {
        closeCalls++;
        events.add("factory");
        if (closeFailure != null) {
            ExceptionUtils.rethrowException(closeFailure);
        }
    }
}
