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

package io.github.flink.gcp.connector.spanner.source.changestream.reader;

import org.apache.flink.annotation.Internal;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import io.github.flink.gcp.connector.spanner.source.changestream.SpannerChangeStreamPartitionSplit;

/** Builds the two dialect-specific Change Streams TVF statements. */
@Internal
final class SpannerChangeStreamStatements {

    private SpannerChangeStreamStatements() {}

    static Statement forSplit(
            Dialect dialect, String streamName, SpannerChangeStreamPartitionSplit split) {
        switch (dialect) {
            case GOOGLE_STANDARD_SQL:
                return googleSql(streamName, split);
            case POSTGRESQL:
                return postgreSql(streamName, split);
            default:
                throw new IllegalStateException(
                        "Unsupported Spanner dialect " + dialect + " for Change Streams.");
        }
    }

    private static Statement googleSql(String streamName, SpannerChangeStreamPartitionSplit split) {
        String function = "READ_" + streamName;
        String sql =
                "SELECT ChangeRecord FROM `"
                        + function.replace("\\", "\\\\").replace("`", "\\`")
                        + "`(start_timestamp => @start_timestamp,"
                        + " end_timestamp => @end_timestamp,"
                        + " partition_token => @partition_token,"
                        + " heartbeat_milliseconds => @heartbeat_milliseconds)";
        return bind(
                Statement.newBuilder(sql),
                split,
                "start_timestamp",
                "end_timestamp",
                "partition_token",
                "heartbeat_milliseconds");
    }

    private static Statement postgreSql(
            String streamName, SpannerChangeStreamPartitionSplit split) {
        String function = "read_json_" + streamName;
        String sql =
                "SELECT * FROM \"spanner\".\""
                        + function.replace("\"", "\"\"")
                        + "\"($1, $2, $3, $4, NULL)";
        return bind(Statement.newBuilder(sql), split, "p1", "p2", "p3", "p4");
    }

    private static Statement bind(
            Statement.Builder builder,
            SpannerChangeStreamPartitionSplit split,
            String start,
            String end,
            String token,
            String heartbeat) {
        builder.bind(start).to(timestamp(split.getCurrentPosition()));
        builder.bind(end).to(timestamp(split.getEndTimestamp()));
        builder.bind(token).to(Value.string(split.getPartitionToken()));
        builder.bind(heartbeat).to(split.getHeartbeatMillis());
        return builder.build();
    }

    private static Value timestamp(java.time.Instant value) {
        return Value.timestamp(
                value == null
                        ? null
                        : Timestamp.ofTimeSecondsAndNanos(value.getEpochSecond(), value.getNano()));
    }
}
