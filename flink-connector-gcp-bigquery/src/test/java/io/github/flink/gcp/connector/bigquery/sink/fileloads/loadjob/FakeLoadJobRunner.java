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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Recording {@link LoadJobRunner} fake with scriptable await failures. */
public final class FakeLoadJobRunner implements LoadJobRunner {

    public final Map<String, LoadJobSpec> loads = new LinkedHashMap<>();
    public final Map<String, CopyJobSpec> copies = new LinkedHashMap<>();
    public final Map<String, QueryJobSpec> queries = new LinkedHashMap<>();
    public final List<String> events = new ArrayList<>();
    public final List<String> awaited = new ArrayList<>();
    public final List<TableDestination> deletedTables = new ArrayList<>();
    public final Set<String> failOnAwait = new HashSet<>();
    public final Map<String, Throwable> failOnAwaitWith = new LinkedHashMap<>();
    public boolean failAllAwaits;
    public long awaitDelayMillis;

    @Override
    public void submitLoad(String jobId, LoadJobSpec spec) {
        events.add("submit-load:" + jobId);
        loads.put(jobId, spec);
    }

    @Override
    public void submitCopy(String jobId, CopyJobSpec spec) {
        events.add("submit-copy:" + jobId);
        copies.put(jobId, spec);
    }

    @Override
    public void submitQuery(String jobId, QueryJobSpec spec) {
        events.add("submit-query:" + jobId);
        queries.put(jobId, spec);
    }

    @Override
    public void awaitJob(String jobId) throws IOException {
        events.add("await:" + jobId);
        awaited.add(jobId);
        try {
            TimeUnit.MILLISECONDS.sleep(awaitDelayMillis);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during scripted job delay", failure);
        }
        Throwable scripted = failOnAwaitWith.get(jobId);
        if (scripted instanceof IOException) {
            throw (IOException) scripted;
        }
        if (scripted instanceof RuntimeException) {
            throw (RuntimeException) scripted;
        }
        if (scripted instanceof Error) {
            throw (Error) scripted;
        }
        if (failAllAwaits || failOnAwait.contains(jobId)) {
            throw new IOException("Job " + jobId + " failed (scripted)");
        }
    }

    @Override
    public void deleteTable(TableDestination table) {
        deletedTables.add(table);
    }
}
