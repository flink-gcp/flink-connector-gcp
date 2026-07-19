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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** The built-in {@link FailedRowHandler} implementations. */
@Internal
final class FailedRowHandlers {

    private FailedRowHandlers() {}

    /** Fails the job on every row-level failure (the default policy). */
    enum FailJob implements FailedRowHandler {
        INSTANCE;

        @Override
        public void handle(FailedRow row) throws IOException {
            throw new IOException(
                    "A row for BigQuery table "
                            + row.getDestination()
                            + " failed terminally: "
                            + row.getErrorMessage(),
                    row.getCause());
        }

        @Override
        public String toString() {
            return "FailedRowHandler.failJob()";
        }
    }

    /** Logs each failed row at WARN level and drops it. */
    enum LogAndDrop implements FailedRowHandler {
        INSTANCE;

        private static final Logger LOG = LoggerFactory.getLogger(LogAndDrop.class);

        @Override
        public void handle(FailedRow row) {
            LOG.warn(
                    "Dropping a row for BigQuery table {} that failed terminally: {}",
                    row.getDestination(),
                    row.getErrorMessage(),
                    row.getCause());
        }

        @Override
        public String toString() {
            return "FailedRowHandler.logAndDrop()";
        }
    }

    /** Routes each failed row to a {@link DeadLetterQueue}. */
    static final class SendToDeadLetterQueue implements FailedRowHandler {

        private static final long serialVersionUID = 1L;

        private final DeadLetterQueue deadLetterQueue;

        SendToDeadLetterQueue(DeadLetterQueue deadLetterQueue) {
            this.deadLetterQueue =
                    Preconditions.checkNotNull(deadLetterQueue, "deadLetterQueue must not be null");
        }

        @Override
        public void handle(FailedRow row) throws IOException {
            deadLetterQueue.offer(row);
        }

        @Override
        public String toString() {
            return "FailedRowHandler.sendToDeadLetterQueue(" + deadLetterQueue + ")";
        }
    }
}
