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

package io.github.flink.gcp.connector.firestore.source.batch.enumerator;

import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** What the planner adds to a partitioning failure; the planning itself is the emulator ITs'. */
class ClientQueryPlannerTest {

    private static final DatabaseDestination DATABASE = DatabaseDestination.of("p", "db");

    private static IOException failure(Status.Code code) {
        return new IOException(
                "Failed to partition the Firestore collection group scan.",
                new StatusRuntimeException(Status.fromCode(code).withDescription("refused")));
    }

    @Test
    void namesOnePartitionWhenTheServiceRefusesAsAPrecondition() {
        IOException failure = failure(Status.Code.FAILED_PRECONDITION);

        IOException planned = ClientQueryPlanner.partitionFailure(failure, DATABASE, 4);

        assertThat(planned)
                .hasMessageContaining("into 4 partitions")
                .hasMessageContaining("partitionCount(1)")
                .hasMessageContaining("'scan.partition.max-partitions' = '1'")
                .hasCause(failure.getCause());
    }

    @Test
    void leavesAnyOtherFailureAsItIs() {
        IOException denied = failure(Status.Code.PERMISSION_DENIED);
        IOException interrupted = new IOException("Interrupted while reading Firestore.");

        assertThat(ClientQueryPlanner.partitionFailure(denied, DATABASE, 4)).isSameAs(denied);
        assertThat(ClientQueryPlanner.partitionFailure(interrupted, DATABASE, 4))
                .isSameAs(interrupted);
    }
}
