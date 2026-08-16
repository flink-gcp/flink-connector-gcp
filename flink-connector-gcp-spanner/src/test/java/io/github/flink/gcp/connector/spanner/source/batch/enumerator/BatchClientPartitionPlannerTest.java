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

package io.github.flink.gcp.connector.spanner.source.batch.enumerator;

import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests of the planner's lifecycle, which is all of it that can be driven without a service.
 *
 * <p>The endpoint is an emulator one throughout, and never connected to: without it the client
 * reaches for application default credentials, which a workstation has and a build agent does not.
 * What it costs a test to reach the service is why the planning call itself is covered by the
 * module's emulator integration tests instead.
 */
class BatchClientPartitionPlannerTest {

    private static final SpannerDatabase DATABASE = SpannerDatabase.of("p", "i", "db");

    @Test
    void planningAfterCloseIsRefusedByName() throws Exception {
        BatchClientPartitionPlanner planner = planner();
        planner.close();

        // Refused before any client is built, which is what keeps a completion arriving after
        // teardown from opening a batch transaction nothing will ever release.
        assertThatThrownBy(this::plan)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("was closed before it was used");

        assertThatThrownBy(
                        () ->
                                planner.plan(
                                        SpannerReadOperation.query(Statement.of("SELECT 1")),
                                        TimestampBound.strong(),
                                        PartitionOptions.getDefaultInstance(),
                                        false,
                                        null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(DATABASE.toString());
    }

    @Test
    void closingTwiceIsHarmless() throws Exception {
        BatchClientPartitionPlanner planner = planner();

        planner.close();

        // The enumerator closes it once, but a teardown that overtook a planning call closes what
        // that call left behind as well — so the second close has to be a no-op rather than a
        // failure the job is told about.
        assertThatCode(planner::close).doesNotThrowAnyException();
    }

    @Test
    void theOptionsCarryOnlyWhatWasAskedFor() {
        // Asserted by kind rather than by value, because the client library reads an option only
        // from inside its own package. What the *value* maps to is SpannerRpcPriorityTest's.
        assertThat(BatchClientPartitionPlanner.queryOptions(false, null)).isEmpty();
        assertThat(BatchClientPartitionPlanner.readOptions(false, null)).isEmpty();

        assertThat(BatchClientPartitionPlanner.queryOptions(false, SpannerRpcPriority.LOW))
                .singleElement()
                .matches(option -> option.getClass() == priorityKind());
        assertThat(BatchClientPartitionPlanner.readOptions(false, SpannerRpcPriority.LOW))
                .singleElement()
                .matches(option -> option.getClass() == priorityKind());

        assertThat(BatchClientPartitionPlanner.queryOptions(true, null))
                .singleElement()
                .matches(option -> option.getClass() == dataBoostKind());
        assertThat(BatchClientPartitionPlanner.readOptions(true, null))
                .singleElement()
                .matches(option -> option.getClass() == dataBoostKind());
    }

    @Test
    void bothOptionsTravelTogether() {
        // The two families are assembled separately because the client gives their values no
        // common supertype; asserting one family would leave the other free to lose an option.
        assertThat(BatchClientPartitionPlanner.queryOptions(true, SpannerRpcPriority.MEDIUM))
                .hasSize(2)
                .anyMatch(option -> option.getClass() == dataBoostKind())
                .anyMatch(option -> option.getClass() == priorityKind());
        assertThat(BatchClientPartitionPlanner.readOptions(true, SpannerRpcPriority.MEDIUM))
                .hasSize(2)
                .anyMatch(option -> option.getClass() == dataBoostKind())
                .anyMatch(option -> option.getClass() == priorityKind());
    }

    /**
     * The class {@code dataBoostEnabled} answers with.
     *
     * <p>Read through the public interface it implements, because the class itself is
     * package-private in the client library — calling {@code getClass()} on the expression does not
     * compile from here.
     */
    private static Class<?> dataBoostKind() {
        Options.ReadAndQueryOption option = Options.dataBoostEnabled(true);
        return option.getClass();
    }

    /** The class {@code priority} answers with. */
    private static Class<?> priorityKind() {
        return Options.priority(Options.RpcPriority.LOW).getClass();
    }

    private void plan() throws IOException {
        BatchClientPartitionPlanner planner = planner();
        planner.close();
        planner.plan(
                SpannerReadOperation.query(Statement.of("SELECT 1")),
                TimestampBound.strong(),
                PartitionOptions.getDefaultInstance(),
                false,
                null);
    }

    private static BatchClientPartitionPlanner planner() {
        return new BatchClientPartitionPlanner(DATABASE, EmulatorEndpoint.parse("localhost:1"));
    }
}
