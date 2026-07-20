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
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileLoadsCheckpointStamper}. */
class FileLoadsCheckpointStamperTest {

    private static final TableDestination T1 = TableDestination.of("p", "d", "t1");

    private static FileLoadsCommittable file(String name) {
        return new FileLoadsCommittable(T1, "gs://bucket/prefix/" + name + ".avro", 10, 5);
    }

    private static List<CommittableMessage<FileLoadsCommittable>> process(
            List<CommittableMessage<FileLoadsCommittable>> messages) throws Exception {
        try (OneInputStreamOperatorTestHarness<
                        CommittableMessage<FileLoadsCommittable>,
                        CommittableMessage<FileLoadsCommittable>>
                harness =
                        new OneInputStreamOperatorTestHarness<>(new FileLoadsCheckpointStamper())) {
            harness.open();
            for (CommittableMessage<FileLoadsCommittable> message : messages) {
                harness.processElement(new StreamRecord<>(message));
            }
            return harness.extractOutputValues().stream()
                    .map(value -> (CommittableMessage<FileLoadsCommittable>) value)
                    .collect(Collectors.toList());
        }
    }

    @Test
    void stampsTheCheckpointIdOntoCommittables() throws Exception {
        List<CommittableMessage<FileLoadsCommittable>> out =
                process(
                        List.of(
                                new CommittableSummary<>(0, 2, 7L, 1, 0),
                                new CommittableWithLineage<>(file("a"), 7L, 0)));

        assertThat(out).hasSize(2);
        assertThat(out.get(0)).isInstanceOf(CommittableSummary.class);
        CommittableWithLineage<FileLoadsCommittable> lineage =
                (CommittableWithLineage<FileLoadsCommittable>) out.get(1);
        assertThat(lineage.getCheckpointId()).isEqualTo(7L);
        assertThat(lineage.getSubtaskId()).isZero();
        assertThat(lineage.getCommittable().getCheckpointId()).isEqualTo(7L);
        assertThat(lineage.getCommittable().getUri()).isEqualTo("gs://bucket/prefix/a.avro");
    }
}
