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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import org.apache.flink.api.connector.sink2.Committer;

import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamCommittable;
import io.github.flink.gcp.connector.bigquery.sink.storage.committer.BufferedStreamCommitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Test-side adapter from writer committables to Flink commit requests. */
final class BufferedStreamCommitTestUtils {

    private BufferedStreamCommitTestUtils() {}

    static void commit(
            BufferedStreamCommitter committer, Collection<BufferedStreamCommittable> committables)
            throws IOException {
        List<Committer.CommitRequest<BufferedStreamCommittable>> requests = new ArrayList<>();
        for (BufferedStreamCommittable committable : committables) {
            requests.add(new TestCommitRequest(committable));
        }
        committer.commit(requests);
    }

    private static final class TestCommitRequest
            implements Committer.CommitRequest<BufferedStreamCommittable> {
        private final BufferedStreamCommittable committable;

        private TestCommitRequest(BufferedStreamCommittable committable) {
            this.committable = committable;
        }

        @Override
        public BufferedStreamCommittable getCommittable() {
            return committable;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
        }

        @Override
        public void signalFailedWithKnownReason(Throwable t) {}

        @Override
        public void signalFailedWithUnknownReason(Throwable t) {}

        @Override
        public void retryLater() {}

        @Override
        public void updateAndRetryLater(BufferedStreamCommittable committable) {}

        @Override
        public void signalAlreadyCommitted() {}
    }
}
