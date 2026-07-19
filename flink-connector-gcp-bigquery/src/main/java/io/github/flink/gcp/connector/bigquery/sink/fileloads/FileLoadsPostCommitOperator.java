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

package io.github.flink.gcp.connector.bigquery.sink.fileloads;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import io.github.flink.gcp.connector.bigquery.sink.BigQuerySinkConfig;
import io.github.flink.gcp.connector.bigquery.sink.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.writer.BigQueryTableAdmin;
import io.github.flink.gcp.connector.bigquery.sink.writer.TableAdmin;

import java.util.ArrayList;
import java.util.List;

/**
 * The parallelism-1 tail of the FILE_LOADS topology: collects every subtask's committables from the
 * post-commit stream and, at end of input — guaranteed, since FILE_LOADS is batch-only — hands them
 * to a {@link LoadJobOrchestrator}.
 *
 * <p>{@link org.apache.flink.streaming.api.connector.sink2.CommittableSummary} messages are
 * bookkeeping and skipped; end of input, not summary completeness, is what triggers loading.
 */
@Internal
public final class FileLoadsPostCommitOperator extends AbstractStreamOperator<Void>
        implements OneInputStreamOperator<CommittableMessage<FileLoadsCommittable>, Void>,
                BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final BigQuerySinkConfig<?> config;
    private final FileLoadsOptions options;
    private final StagingStorage storage;

    private transient List<FileLoadsCommittable> collected;
    private transient LoadJobRunner runner;
    private transient TableAdmin tableAdmin;

    public FileLoadsPostCommitOperator(
            BigQuerySinkConfig<?> config, FileLoadsOptions options, StagingStorage storage) {
        this.config = config;
        this.options = options;
        this.storage = storage;
    }

    @VisibleForTesting
    FileLoadsPostCommitOperator(
            BigQuerySinkConfig<?> config,
            FileLoadsOptions options,
            StagingStorage storage,
            LoadJobRunner runner,
            TableAdmin tableAdmin) {
        this(config, options, storage);
        this.runner = runner;
        this.tableAdmin = tableAdmin;
    }

    @Override
    public void open() throws Exception {
        super.open();
        collected = new ArrayList<>();
    }

    @Override
    public void processElement(StreamRecord<CommittableMessage<FileLoadsCommittable>> element) {
        if (element.getValue() instanceof CommittableWithLineage) {
            collected.add(
                    ((CommittableWithLineage<FileLoadsCommittable>) element.getValue())
                            .getCommittable());
        }
    }

    @Override
    public void endInput() throws Exception {
        if (runner == null) {
            runner = new BigQueryLoadJobRunner(config.getLocation());
        }
        if (tableAdmin == null) {
            tableAdmin = new BigQueryTableAdmin();
        }
        String flinkJobId = getRuntimeContext().getJobInfo().getJobId().toString();
        new LoadJobOrchestrator(config, options, runner, tableAdmin, storage, flinkJobId)
                .run(collected);
        collected = new ArrayList<>();
    }
}
