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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;

import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;

/**
 * The streaming pre-commit stage of the FILE_LOADS topology: stamps each committable with the
 * checkpoint id carried by its {@link CommittableMessage}, which the {@link
 * org.apache.flink.api.connector.sink2.Committer} SPI cannot observe otherwise — the stamp selects
 * the streaming behavior of the load-job orchestrator and makes BigQuery job ids attributable to
 * their checkpoint. Stateless and chained to the writer; batch execution has no stamping stage.
 */
@Internal
public final class FileLoadsCheckpointStamper
        implements MapFunction<
                CommittableMessage<FileLoadsCommittable>,
                CommittableMessage<FileLoadsCommittable>> {

    private static final long serialVersionUID = 1L;

    @Override
    public CommittableMessage<FileLoadsCommittable> map(
            CommittableMessage<FileLoadsCommittable> message) {
        if (message instanceof CommittableWithLineage) {
            CommittableWithLineage<FileLoadsCommittable> lineage =
                    (CommittableWithLineage<FileLoadsCommittable>) message;
            // Through the seam rather than off the message directly: the accessor that reads a
            // checkpoint id is spelled differently on each supported Flink major, and the one
            // spelling both accept is the one 2.x has announced for removal.
            // CrossVersionCheckpointId is one file per compat source root (ADR-0054; issue #404).
            return lineage.map(
                    committable ->
                            committable.withCheckpointId(CrossVersionCheckpointId.of(lineage)));
        }
        return message;
    }
}
