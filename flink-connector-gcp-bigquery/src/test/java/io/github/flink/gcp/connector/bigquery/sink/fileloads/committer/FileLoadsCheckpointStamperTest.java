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

import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileLoadsCheckpointStamper}. */
class FileLoadsCheckpointStamperTest {

    private static final String FLINK_JOB_ID = "0123456789abcdef0123456789abcdef";
    private static final TableDestination T1 = TableDestination.of("p", "d", "t1");

    private final FileLoadsCheckpointStamper stamper = new FileLoadsCheckpointStamper();

    private static FileLoadsCommittable file(String name) {
        return new FileLoadsCommittable(
                FLINK_JOB_ID, T1, "gs://bucket/prefix/" + name + ".avro", 10, 5);
    }

    @Test
    void stampsTheCheckpointIdOntoCommittables() {
        CommittableMessage<FileLoadsCommittable> out =
                stamper.map(new CommittableWithLineage<>(file("a"), 7L, 3));

        CommittableWithLineage<FileLoadsCommittable> lineage =
                (CommittableWithLineage<FileLoadsCommittable>) out;
        assertThat(lineage.getCheckpointId()).isEqualTo(7L);
        assertThat(lineage.getSubtaskId()).isEqualTo(3);
        assertThat(lineage.getCommittable().getCheckpointId()).isEqualTo(7L);
        assertThat(lineage.getCommittable().getUri()).isEqualTo("gs://bucket/prefix/a.avro");
        assertThat(lineage.getCommittable().getFlinkJobId()).isEqualTo(FLINK_JOB_ID);
    }

    @Test
    void forwardsSummariesUntouched() {
        CommittableSummary<FileLoadsCommittable> summary = new CommittableSummary<>(0, 2, 7L, 1, 0);

        assertThat(stamper.map(summary)).isSameAs(summary);
    }
}
