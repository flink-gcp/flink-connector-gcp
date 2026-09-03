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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import org.apache.flink.annotation.Internal;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;

/**
 * The two request-response RPCs of a Bigtable data client, as the single-row runtime issues them.
 *
 * <p>The seam exists because {@code BigtableDataClient} has no public constructor and nothing about
 * it reports which instance it talks to, so a test can neither build one nor tell two apart. The
 * production implementation is an adapter over the client's own {@code checkAndMutateRowAsync} and
 * {@code readModifyWriteRowAsync}; a fake answers from settable futures.
 *
 * <p>Each call is one attempt against the client's settings — the factory that builds the client
 * pins both RPCs to a single attempt under the configured deadline — and the returned future is the
 * client's own, so cancelling it cancels the RPC. Neither RPC is retried by the client or the
 * runtime: both are non-idempotent and ship with an empty retryable-code set (ADR-0148).
 */
@Internal
public interface SingleRowClient {

    /**
     * Issues a {@code CheckAndMutateRow}.
     *
     * @param mutation the conditional mutation, carrying its table id
     * @return whether the condition matched, once the service answers
     */
    ApiFuture<Boolean> checkAndMutateRow(ConditionalRowMutation mutation);

    /**
     * Issues a {@code ReadModifyWriteRow}.
     *
     * @param mutation the read-modify-write rules, carrying their table id
     * @return the cells the rules touched, once the service answers
     */
    ApiFuture<Row> readModifyWriteRow(ReadModifyWriteRow mutation);
}
