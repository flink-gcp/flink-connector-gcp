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

package io.github.flink.gcp.connector.spanner.source.changestream.reader;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import io.github.flink.gcp.connector.spanner.source.changestream.ChangeStreamPartitionSplit;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class SpannerChangeStreamStatementsTest {

    @Test
    void googleSqlQuotesTheFunctionAndBindsEveryValue() {
        Statement statement =
                SpannerChangeStreamStatements.forSplit(
                        Dialect.GOOGLE_STANDARD_SQL, "Odd`Name", split());

        assertThat(statement.getSql())
                .startsWith("SELECT ChangeRecord FROM `READ_Odd\\`Name`")
                .contains("@start_timestamp")
                .contains("@end_timestamp")
                .contains("@partition_token")
                .contains("@heartbeat_milliseconds");
        assertThat(statement.getParameters())
                .containsOnlyKeys(
                        "start_timestamp",
                        "end_timestamp",
                        "partition_token",
                        "heartbeat_milliseconds");
        assertThat(statement.getParameters().get("partition_token").getString()).isEqualTo("token");
        assertThat(statement.getParameters().get("heartbeat_milliseconds").getInt64())
                .isEqualTo(2_000);
    }

    @Test
    void postgreSqlDoublesQuotesAndUsesPositionalBindings() {
        Statement statement =
                SpannerChangeStreamStatements.forSplit(Dialect.POSTGRESQL, "Odd\"Name", split());

        assertThat(statement.getSql())
                .isEqualTo(
                        "SELECT * FROM \"spanner\".\"read_json_Odd\"\"Name\""
                                + "($1, $2, $3, $4, NULL)");
        assertThat(statement.getParameters()).containsOnlyKeys("p1", "p2", "p3", "p4");
        assertThat(statement.getParameters().get("p3").getString()).isEqualTo("token");
    }

    @Test
    void initialUnboundedQueryBindsTypedNulls() {
        ChangeStreamPartitionSplit initial =
                ChangeStreamPartitionSplit.initial(
                        Instant.parse("2026-01-01T00:00:00Z"), null, 2_000);

        Statement statement =
                SpannerChangeStreamStatements.forSplit(
                        Dialect.GOOGLE_STANDARD_SQL, "changes", initial);

        assertThat(statement.getParameters().get("end_timestamp").isNull()).isTrue();
        assertThat(statement.getParameters().get("partition_token").isNull()).isTrue();
    }

    private static ChangeStreamPartitionSplit split() {
        return new ChangeStreamPartitionSplit(
                "token",
                java.util.Collections.singletonList(
                        ChangeStreamPartitionSplit.INITIAL_PARTITION_ID),
                Instant.parse("2026-01-01T00:00:00Z"),
                Instant.parse("2026-01-01T01:00:00Z"),
                2_000,
                Instant.parse("2026-01-01T00:10:00Z"),
                io.github.flink.gcp.connector.spanner.source.changestream.PartitionLifecycleState
                        .RUNNING,
                Instant.parse("2026-01-01T00:09:00Z"));
    }
}
