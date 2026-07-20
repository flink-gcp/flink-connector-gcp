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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.committer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Committer;

import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;

import java.util.Collection;

/**
 * A deliberate no-op: staging files are already finalized when the writer emits their committables,
 * and the actual commit — the load jobs — needs a global per-table view of every subtask's files,
 * so it runs in the (parallelism-1) post-commit topology instead of this per-subtask committer. The
 * committer exists because {@link org.apache.flink.api.connector.sink2.SupportsCommitter} is what
 * makes committables flow into the post-commit stream.
 */
@Internal
public final class FileLoadsCommitter implements Committer<FileLoadsCommittable> {

    @Override
    public void commit(Collection<CommitRequest<FileLoadsCommittable>> committables) {
        // Requests left unsignaled are treated as committed.
    }

    @Override
    public void close() {}
}
