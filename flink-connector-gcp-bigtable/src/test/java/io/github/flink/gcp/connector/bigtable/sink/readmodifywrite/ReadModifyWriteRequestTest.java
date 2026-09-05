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

package io.github.flink.gcp.connector.bigtable.sink.readmodifywrite;

import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReadModifyWriteRequestTest {
    private static final ByteString KEY = ByteString.copyFrom(new byte[] {0, -1, 2});
    private static final ByteString QUALIFIER = ByteString.copyFrom(new byte[] {-1, 0});
    private static final RequestContext CONTEXT = RequestContext.create("p", "i", "profile");

    @Test
    void immutableSerializableRulesPreserveDuplicateColumnsAndExactWireOrder() throws Exception {
        List<ReadModifyWriteRule> rules =
                new ArrayList<>(
                        List.of(
                                ReadModifyWriteRule.append(
                                        "cf", QUALIFIER, ByteString.copyFromUtf8("first")),
                                ReadModifyWriteRule.increment(
                                        "counts", ByteString.EMPTY, Long.MIN_VALUE),
                                ReadModifyWriteRule.append(
                                        "cf", QUALIFIER, ByteString.copyFrom(new byte[] {0, -1})),
                                ReadModifyWriteRule.increment(
                                        "counts", ByteString.EMPTY, Long.MAX_VALUE),
                                ReadModifyWriteRule.increment("counts", ByteString.EMPTY, 0)));
        ReadModifyWriteRequest request = ReadModifyWriteRequest.of(KEY, rules);
        rules.clear();
        assertThatThrownBy(() -> request.getRules().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        ReadModifyWriteRequest restored =
                InstantiationUtil.clone(request, getClass().getClassLoader());
        FakeReadModifyWriteClients client = new FakeReadModifyWriteClients();
        restored.toRequest().start(client, TableDestination.of("p", "i", "resolved"));
        com.google.bigtable.v2.ReadModifyWriteRowRequest sent = client.sent.get(0).toProto(CONTEXT);
        assertThat(sent.getTableName()).isEqualTo("projects/p/instances/i/tables/resolved");
        assertThat(sent.getAppProfileId()).isEqualTo("profile");
        assertThat(sent.getRowKey()).isEqualTo(KEY);
        assertThat(sent.getRulesList())
                .containsExactly(
                        com.google.bigtable.v2.ReadModifyWriteRule.newBuilder()
                                .setFamilyName("cf")
                                .setColumnQualifier(QUALIFIER)
                                .setAppendValue(ByteString.copyFromUtf8("first"))
                                .build(),
                        com.google.bigtable.v2.ReadModifyWriteRule.newBuilder()
                                .setFamilyName("counts")
                                .setColumnQualifier(ByteString.EMPTY)
                                .setIncrementAmount(Long.MIN_VALUE)
                                .build(),
                        com.google.bigtable.v2.ReadModifyWriteRule.newBuilder()
                                .setFamilyName("cf")
                                .setColumnQualifier(QUALIFIER)
                                .setAppendValue(ByteString.copyFrom(new byte[] {0, -1}))
                                .build(),
                        com.google.bigtable.v2.ReadModifyWriteRule.newBuilder()
                                .setFamilyName("counts")
                                .setColumnQualifier(ByteString.EMPTY)
                                .setIncrementAmount(Long.MAX_VALUE)
                                .build(),
                        com.google.bigtable.v2.ReadModifyWriteRule.newBuilder()
                                .setFamilyName("counts")
                                .setColumnQualifier(ByteString.EMPTY)
                                .setIncrementAmount(0)
                                .build());
    }

    @Test
    void rejectsInvalidRequestsBeforeAnyClientIsNeeded() {
        ReadModifyWriteRule rule = ReadModifyWriteRule.increment("cf", QUALIFIER, -1);
        assertThat(ReadModifyWriteRequest.of(KEY, List.of(rule)).getRules()).containsExactly(rule);
        assertThat(ReadModifyWriteRequest.of(KEY, Collections.nCopies(100000, rule)).getRules())
                .hasSize(100000);
        assertThatThrownBy(() -> ReadModifyWriteRequest.of(KEY, Collections.nCopies(100001, rule)))
                .hasMessageContaining("100000");
        assertThatThrownBy(() -> ReadModifyWriteRequest.of(KEY, List.of()))
                .hasMessageContaining("between 1");
        assertThatThrownBy(() -> ReadModifyWriteRequest.of(ByteString.EMPTY, List.of(rule)))
                .hasMessageContaining("rowKey");
        assertThatThrownBy(() -> ReadModifyWriteRequest.of(null, List.of(rule)))
                .hasMessageContaining("rowKey");
        assertThatThrownBy(() -> ReadModifyWriteRequest.of(KEY, null))
                .hasMessageContaining("rules");
        assertThatThrownBy(() -> ReadModifyWriteRequest.of(KEY, Arrays.asList(rule, null)))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> ReadModifyWriteRule.append("cf", QUALIFIER, ByteString.EMPTY))
                .hasMessageContaining("value must not be empty");
        assertThatThrownBy(() -> ReadModifyWriteRule.append("cf", QUALIFIER, null))
                .hasMessageContaining("value");
        assertThatThrownBy(() -> ReadModifyWriteRule.increment(" ", QUALIFIER, 0))
                .hasMessageContaining("family");
        assertThatThrownBy(() -> ReadModifyWriteRule.increment("cf", null, 0))
                .hasMessageContaining("qualifier");
    }
}
