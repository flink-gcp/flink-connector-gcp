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

package io.github.flink.gcp.connector.base.failure;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** The built-in {@link FailureHandler} implementations. */
@Internal
final class FailureHandlers {

    private FailureHandlers() {}

    /** Fails the job on every per-element terminal failure (the default policy). */
    enum FailJob implements FailureHandler<FailedElement> {
        INSTANCE;

        @Override
        public void handle(FailedElement element) throws IOException {
            throw new IOException(
                    "A record for "
                            + element.getConnector()
                            + " destination "
                            + element.describeDestination()
                            + " failed terminally: "
                            + element.getErrorMessage(),
                    element.getCause());
        }

        @Override
        public String toString() {
            return "FailureHandler.failJob()";
        }
    }

    /** Logs each failed element at WARN level and drops it. */
    enum LogAndDrop implements FailureHandler<FailedElement> {
        INSTANCE;

        private static final Logger LOG = LoggerFactory.getLogger(LogAndDrop.class);

        @Override
        public void handle(FailedElement element) {
            LOG.warn(
                    "Dropping a record for {} destination {} that failed terminally: {}",
                    element.getConnector(),
                    element.describeDestination(),
                    element.getErrorMessage(),
                    element.getCause());
        }

        @Override
        public String toString() {
            return "FailureHandler.logAndDrop()";
        }
    }

    /** Routes each failed element to a {@link DeadLetterQueue}, driving the queue's lifecycle. */
    static final class SendToDeadLetterQueue implements FailureHandler<FailedElement> {

        private static final long serialVersionUID = 1L;

        private final DeadLetterQueue deadLetterQueue;

        SendToDeadLetterQueue(DeadLetterQueue deadLetterQueue) {
            this.deadLetterQueue =
                    Preconditions.checkNotNull(deadLetterQueue, "deadLetterQueue must not be null");
        }

        @Override
        public void handle(FailedElement element) throws IOException {
            deadLetterQueue.offer(element);
        }

        @Override
        public void open(FailureHandlerContext context) throws IOException {
            deadLetterQueue.open(context);
        }

        @Override
        public void flush() throws IOException {
            deadLetterQueue.flush();
        }

        @Override
        public void close() throws Exception {
            deadLetterQueue.close();
        }

        @Override
        public String toString() {
            return "FailureHandler.sendToDeadLetterQueue(" + deadLetterQueue + ")";
        }
    }
}
