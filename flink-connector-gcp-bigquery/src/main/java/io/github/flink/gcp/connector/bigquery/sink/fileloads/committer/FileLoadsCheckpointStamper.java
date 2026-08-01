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
            // getCheckpointIdOrEOI, not getCheckpointId: the latter's return type differs across
            // the supported Flink majors (OptionalLong in 1.20, long in 2.x), while this accessor
            // returns long in both. Deprecated on 2.x as a plain alias of getCheckpointId — if a
            // future 2.x minor removes it, this is the one line to revisit (issue #32).
            return lineage.map(
                    committable -> committable.withCheckpointId(lineage.getCheckpointIdOrEOI()));
        }
        return message;
    }
}
