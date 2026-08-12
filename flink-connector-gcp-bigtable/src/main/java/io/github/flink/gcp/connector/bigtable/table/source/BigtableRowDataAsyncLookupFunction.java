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

package io.github.flink.gcp.connector.bigtable.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import io.github.flink.gcp.connector.bigtable.table.CellValueCodec;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** An asynchronous Bigtable row-key point lookup producing the table source's projected row. */
@Internal
public final class BigtableRowDataAsyncLookupFunction extends AsyncLookupFunction {

    private static final long serialVersionUID = 1L;

    private final CellValueCodec.FieldEncoder rowKeyEncoder;
    private final RowToRowDataConverter converter;
    private final int maxRetries;
    private final BigtableRowLookup lookup;

    BigtableRowDataAsyncLookupFunction(
            TableDestination destination,
            BigtableTableSchema schema,
            @Nullable int[] projectedFields,
            String nullStringLiteral,
            Filters.Filter filter,
            List<ByteStringRange> ranges,
            @Nullable String appProfileId,
            @Nullable String serviceAccountKeyFile,
            @Nullable String emulatorEndpoint,
            int maxRetries) {
        this(
                schema,
                projectedFields,
                nullStringLiteral,
                maxRetries,
                new BigtableDataClientRowLookup(
                        destination,
                        filter,
                        ranges,
                        appProfileId,
                        serviceAccountKeyFile,
                        emulatorEndpoint));
    }

    @VisibleForTesting
    BigtableRowDataAsyncLookupFunction(
            BigtableTableSchema schema,
            @Nullable int[] projectedFields,
            String nullStringLiteral,
            int maxRetries,
            BigtableRowLookup lookup) {
        this.rowKeyEncoder = CellValueCodec.encoder(schema.getRowKeyType());
        this.converter = new RowToRowDataConverter(schema, projectedFields, nullStringLiteral);
        this.maxRetries = maxRetries;
        this.lookup = lookup;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        lookup.open();
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        if (keyRow.isNullAt(0)) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        ByteString rowKey = ByteString.copyFrom(rowKeyEncoder.encode(keyRow, 0));
        CompletableFuture<Collection<RowData>> result = new CompletableFuture<>();
        new LookupAttempt(rowKey, result).schedule();
        return result;
    }

    /** One lookup's stack-safe retry trampoline and active-call cancellation handle. */
    private final class LookupAttempt {

        private final ByteString rowKey;
        private final CompletableFuture<Collection<RowData>> result;
        private final AtomicInteger work = new AtomicInteger();
        private final AtomicReference<ApiFuture<Row>> active = new AtomicReference<>();
        private int retry;

        private LookupAttempt(ByteString rowKey, CompletableFuture<Collection<RowData>> result) {
            this.rowKey = rowKey;
            this.result = result;
            result.whenComplete(
                    (ignored, failure) -> {
                        if (result.isCancelled()) {
                            ApiFuture<Row> call = active.get();
                            if (call != null) {
                                call.cancel(true);
                            }
                        }
                    });
        }

        private void schedule() {
            if (work.getAndIncrement() != 0) {
                return;
            }
            int pending = 1;
            do {
                if (!result.isDone()) {
                    issue();
                }
                pending = work.addAndGet(-pending);
            } while (pending != 0);
        }

        private void issue() {
            final ApiFuture<Row> readFuture;
            try {
                readFuture = lookup.readAsync(rowKey);
            } catch (RuntimeException failure) {
                handleFailure(failure);
                return;
            }
            active.set(readFuture);
            if (result.isCancelled()) {
                readFuture.cancel(true);
                return;
            }
            ApiFutures.addCallback(
                    readFuture,
                    new ApiFutureCallback<Row>() {
                        @Override
                        public void onFailure(Throwable failure) {
                            active.compareAndSet(readFuture, null);
                            handleFailure(failure);
                        }

                        @Override
                        public void onSuccess(@Nullable Row row) {
                            active.compareAndSet(readFuture, null);
                            try {
                                result.complete(convert(row));
                            } catch (RuntimeException failure) {
                                result.completeExceptionally(failure);
                            }
                        }
                    },
                    Runnable::run);
        }

        private void handleFailure(Throwable failure) {
            if (result.isDone()) {
                return;
            }
            if (retry < maxRetries && BigtableLookupErrorClassifier.isTransient(failure)) {
                retry++;
                schedule();
            } else {
                result.completeExceptionally(failure);
            }
        }
    }

    private Collection<RowData> convert(@Nullable Row row) {
        return row == null
                ? Collections.emptyList()
                : Collections.singletonList(converter.convert(row));
    }

    @Override
    public void close() throws Exception {
        lookup.close();
    }
}
