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

package io.github.flink.gcp.connector.bigtable.sink.conditional;

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowClient;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowClientFactory;

import java.util.ArrayList;
import java.util.List;

/** Request and client lifecycle recorder shared by conditional API tests. */
final class ConditionalTestClients implements SingleRowClientFactory, SingleRowClient {
    private static final long serialVersionUID = 1L;
    final List<ConditionalRowMutation> sent = new ArrayList<>();
    final List<SettableApiFuture<Boolean>> answers = new ArrayList<>();
    final List<TableDestination> opened = new ArrayList<>();
    int closes;

    @Override
    public SingleRowClient create(TableDestination destination) {
        opened.add(destination);
        return this;
    }

    @Override
    public void release(TableDestination destination) {}

    @Override
    public void close() {
        closes++;
    }

    @Override
    public ApiFuture<Boolean> checkAndMutateRow(ConditionalRowMutation request) {
        sent.add(request);
        SettableApiFuture<Boolean> answer = SettableApiFuture.create();
        answers.add(answer);
        return answer;
    }

    @Override
    public ApiFuture<Row> readModifyWriteRow(ReadModifyWriteRow request) {
        throw new AssertionError("Wrong RPC");
    }
}
