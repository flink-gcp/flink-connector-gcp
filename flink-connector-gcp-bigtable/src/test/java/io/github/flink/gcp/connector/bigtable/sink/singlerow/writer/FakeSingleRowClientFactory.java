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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import org.apache.flink.util.ExceptionUtils;

import io.github.flink.gcp.connector.bigtable.TableDestination;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link SingleRowClientFactory} leasing one {@link FakeSingleRowClient} per instance and
 * recording every lease, release and close, so a runtime test can assert what it leased and when it
 * gave it back.
 *
 * <p>Failures are scripted per call: {@link #createFailures} feeds the next leases one throw each,
 * and {@link #releaseFailure} and {@link #closeFailure} make every release or close throw.
 */
final class FakeSingleRowClientFactory implements SingleRowClientFactory {

    private static final long serialVersionUID = 1L;

    /** The clients, one per instance, in creation order. */
    final Map<String, FakeSingleRowClient> clients = new LinkedHashMap<>();

    /** The tables the runtime leased a client for, in call order, repeats included. */
    final List<TableDestination> created = new ArrayList<>();

    /** The tables whose leases the runtime released, in call order. */
    final List<TableDestination> released = new ArrayList<>();

    /** Failures the next {@link #create} calls throw, one each, until empty. */
    final Deque<Exception> createFailures = new ArrayDeque<>();

    int closeCalls;
    @Nullable Throwable releaseFailure;
    @Nullable Throwable closeFailure;

    /** Returns (creating if needed) the client of a table's instance, so a test can script it. */
    FakeSingleRowClient clientFor(TableDestination destination) {
        return clients.computeIfAbsent(instanceKey(destination), FakeSingleRowClient::new);
    }

    @Override
    public SingleRowClient create(TableDestination destination)
            throws IOException, InterruptedException {
        Exception failure = createFailures.poll();
        if (failure != null) {
            ExceptionUtils.rethrowIOException(failure);
        }
        created.add(destination);
        return clientFor(destination);
    }

    @Override
    public void release(TableDestination destination) throws Exception {
        released.add(destination);
        if (releaseFailure != null) {
            ExceptionUtils.rethrowException(releaseFailure);
        }
    }

    @Override
    public void close() throws Exception {
        closeCalls++;
        if (closeFailure != null) {
            ExceptionUtils.rethrowException(closeFailure);
        }
    }

    private static String instanceKey(TableDestination destination) {
        return destination.getProject() + "/" + destination.getInstance();
    }
}
