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

package io.github.flink.gcp.connector.tier3.cloudtasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;

import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksCreateTaskSink;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksStagedCreateTaskSink;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.DefaultTaskCreatorFactory;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.TaskCreator;

import java.io.IOException;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

/** Uses existing runtime injection seams without replacing the writer or committer. */
@Internal
final class ObservedSinks {
    private ObservedSinks() {}

    @SuppressWarnings("unchecked")
    static Sink<Long> observe(Sink<Long> sink, MeasurementOptions options) {
        if (sink instanceof CloudTasksStagedCreateTaskSink) {
            return new Staged((CloudTasksStagedCreateTaskSink<Long>) sink, options);
        }
        return new Eager((CloudTasksCreateTaskSink<Long>) sink, options);
    }

    private static TaskCreator observe(TaskCreator creator, MeasurementOptions options)
            throws IOException {
        return observe(creator, options, MeasurementReceipts.storage(options));
    }

    static TaskCreator observe(
            TaskCreator creator, MeasurementOptions options, MeasurementReceipts.Output receipts)
            throws IOException {
        return observe(creator, options, receipts, incarnation -> rows(options, incarnation));
    }

    /** Window mode exports rows to storage beside the receipts; record-count mode prints them. */
    static ObservationLog.Output rows(MeasurementOptions options, UUID incarnation) {
        return options.windowed()
                ? RowsOutput.storage(options.rowsPrefix(), incarnation, System::nanoTime)
                : ObservationLog.Output.stdout();
    }

    static TaskCreator observe(
            TaskCreator creator,
            MeasurementOptions options,
            MeasurementReceipts.Output receipts,
            Function<UUID, ObservationLog.Output> rows)
            throws IOException {
        UUID incarnation = UUID.randomUUID();
        Consumer<ObservedTaskCreator.Terminal> terminal = ignored -> {};
        ObservationLog log;
        try {
            if (options.windowed()) {
                terminal = MeasurementReceipts.creator(options, incarnation, receipts);
            }
            log = new ObservationLog(options, incarnation, rows.apply(incarnation));
        } catch (Throwable failure) {
            Closers.closeAllSuppressing(failure, creator);
            throw failure;
        }
        TaskCreator observed =
                new ObservedTaskCreator(
                        creator, log, System::nanoTime, options.attemptLimit, terminal);
        return options.arm == MeasurementOptions.Arm.NAMED_RANDOM_CONTROL
                ? new RandomNameControl(observed)
                : observed;
    }

    private static final class Eager extends CloudTasksCreateTaskSink<Long> {
        private static final long serialVersionUID = 1L;
        private final MeasurementOptions options;

        private Eager(CloudTasksCreateTaskSink<Long> sink, MeasurementOptions options) {
            super(sink.getConfig());
            this.options = options;
        }

        @Override
        public DefaultTaskCreatorFactory taskCreatorFactory() {
            return new DefaultTaskCreatorFactory(
                    getConfig().getServiceAccountKeyFile(),
                    getConfig().getEmulatorEndpoint(),
                    getConfig().getWriterOptions().getChannelPoolSize()) {
                @Override
                public TaskCreator create() throws IOException {
                    return observe(super.create(), options);
                }
            };
        }
    }

    private static final class Staged extends CloudTasksStagedCreateTaskSink<Long> {
        private static final long serialVersionUID = 1L;
        private final MeasurementOptions options;

        private Staged(CloudTasksStagedCreateTaskSink<Long> sink, MeasurementOptions options) {
            super(sink.getConfig(), sink.getStagedOptions(), null);
            this.options = options;
        }

        @Override
        protected TaskCreator createTaskCreator(DefaultTaskCreatorFactory factory)
                throws IOException {
            return observe(super.createTaskCreator(factory), options);
        }
    }
}
