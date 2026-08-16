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

package io.github.flink.gcp.connector.bigtable;

import com.google.bigtable.admin.v2.ChangeStreamConfig;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.admin.v2.models.UpdateTableRequest;
import com.google.protobuf.Duration;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AbstractBigtableRealGcpITCaseTest {

    @Test
    void disablesOnlyTablesThatHaveChangeStreamsBeforeInstanceDeletion() {
        List<UpdateTableRequest> updates = new ArrayList<>();
        Map<String, Table> tables = new LinkedHashMap<>();
        tables.put("plain", table("plain", false));
        tables.put("streamed", table("streamed", true));

        AbstractBigtableRealGcpITCase.disableChangeStreams(
                new ArrayList<>(tables.keySet()), tables::get, updates::add);

        assertThat(updates).hasSize(1);
        com.google.bigtable.admin.v2.UpdateTableRequest request =
                updates.get(0).toProto("project", "instance");
        assertThat(request.getTable().getName())
                .isEqualTo("projects/project/instances/instance/tables/streamed");
        assertThat(request.getTable().hasChangeStreamConfig()).isFalse();
        assertThat(request.getUpdateMask().getPathsList()).containsExactly("change_stream_config");
    }

    private static Table table(String id, boolean changeStreams) {
        com.google.bigtable.admin.v2.Table.Builder table =
                com.google.bigtable.admin.v2.Table.newBuilder()
                        .setName("projects/project/instances/instance/tables/" + id);
        if (changeStreams) {
            table.setChangeStreamConfig(
                    ChangeStreamConfig.newBuilder()
                            .setRetentionPeriod(Duration.newBuilder().setSeconds(86_400)));
        }
        return Table.fromProto(table.build());
    }
}
