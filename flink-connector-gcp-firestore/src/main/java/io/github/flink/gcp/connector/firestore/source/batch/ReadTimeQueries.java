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

package io.github.flink.gcp.connector.firestore.source.batch;

import org.apache.flink.annotation.Internal;

import com.google.api.core.ApiFuture;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.TransactionOptions;

/**
 * Runs a query at a read time: the one read both the planner's probe and the readers' pages make.
 *
 * <p>A read-only transaction with a read time is the only public way to read at one; the client
 * library runs it locally, with no service transaction, and sets the time on the request. It is
 * started asynchronously so that a caller can wait for it interruptibly and cancel its wait: the
 * synchronous form runs the read on the caller's thread inside the library and turns an interrupt
 * into an ordinary failure.
 */
@Internal
public final class ReadTimeQueries {

    private ReadTimeQueries() {}

    /**
     * Starts a read of the whole of a query at a read time.
     *
     * @param query the query, limited by the caller
     * @param readTime the snapshot time
     * @return the read's result, once it completes
     */
    public static ApiFuture<QuerySnapshot> start(Query query, Timestamp readTime) {
        return query.getFirestore()
                .runAsyncTransaction(
                        transaction -> transaction.get(query),
                        TransactionOptions.createReadOnlyOptionsBuilder()
                                .setReadTime(readTime.toProto())
                                .build());
    }
}
