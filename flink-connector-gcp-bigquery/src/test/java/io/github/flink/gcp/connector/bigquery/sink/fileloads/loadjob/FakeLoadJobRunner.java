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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Recording {@link LoadJobRunner} fake with scriptable await failures. */
final class FakeLoadJobRunner implements LoadJobRunner {

    final Map<String, LoadJobSpec> loads = new LinkedHashMap<>();
    final Map<String, CopyJobSpec> copies = new LinkedHashMap<>();
    final List<String> awaited = new ArrayList<>();
    final List<TableDestination> deletedTables = new ArrayList<>();
    final Set<String> failOnAwait = new HashSet<>();
    boolean failAllAwaits;

    @Override
    public void submitLoad(String jobId, LoadJobSpec spec) {
        loads.put(jobId, spec);
    }

    @Override
    public void submitCopy(String jobId, CopyJobSpec spec) {
        copies.put(jobId, spec);
    }

    @Override
    public void awaitJob(String jobId) throws IOException {
        awaited.add(jobId);
        if (failAllAwaits || failOnAwait.contains(jobId)) {
            throw new IOException("Job " + jobId + " failed (scripted)");
        }
    }

    @Override
    public void deleteTable(TableDestination table) {
        deletedTables.add(table);
    }
}
