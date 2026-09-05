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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.options.OptionChecks;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Bounded conditional requests emitting the original input and its successful response. Delivery is
 * at-least-once; a replay can select a different branch. No async retry entry points or failure
 * handlers are provided. Requires single-cluster routing and single-row transactions.
 *
 * @param <T> the input type
 */
@PublicEvolving
public final class BigtableConditionalAsync<T> {
    private final ConditionalConfig<T> config;

    BigtableConditionalAsync(ConditionalConfig<T> config) {
        this.config = config;
    }

    /**
     * Creates a builder requiring a destination and serialization schema.
     *
     * @param <T> the input type
     * @return the builder
     */
    public static <T> BigtableConditionalAsyncBuilder<T> builder() {
        return new BigtableConditionalAsyncBuilder<>();
    }

    /**
     * Emits successful responses in input order. This does not order RPC execution for the same
     * row.
     *
     * @param input the input stream
     * @param timeout the positive Flink timeout, greater than requestTimeout after truncation to
     *     milliseconds and representable in nanoseconds
     * @return input/response pairs; serializer skips emit nothing
     */
    public SingleOutputStreamOperator<Tuple2<T, ConditionalResult>> orderedWait(
            DataStream<T> input, Duration timeout) {
        return AsyncDataStream.orderedWait(
                        input,
                        function(),
                        timeoutNanos(timeout),
                        TimeUnit.NANOSECONDS,
                        config.requestOptions.getMaxInFlightRequests())
                .returns(resultType(input));
    }

    /**
     * Emits successful responses as they arrive, subject to Flink's watermark ordering.
     *
     * @param input the input stream
     * @param timeout the positive Flink timeout, greater than requestTimeout after truncation to
     *     milliseconds and representable in nanoseconds
     * @return input/response pairs; serializer skips emit nothing
     */
    public SingleOutputStreamOperator<Tuple2<T, ConditionalResult>> unorderedWait(
            DataStream<T> input, Duration timeout) {
        return AsyncDataStream.unorderedWait(
                        input,
                        function(),
                        timeoutNanos(timeout),
                        TimeUnit.NANOSECONDS,
                        config.requestOptions.getMaxInFlightRequests())
                .returns(resultType(input));
    }

    ConditionalFunction<T> function() {
        return new ConditionalFunction<>(config);
    }

    private long timeoutNanos(Duration timeout) {
        Preconditions.checkNotNull(timeout, "timeout must not be null");
        OptionChecks.checkExpressibleInNanos(timeout, "timeout");
        Preconditions.checkArgument(
                Duration.ofMillis(timeout.toMillis())
                                .compareTo(config.requestOptions.getRequestTimeout())
                        > 0,
                "timeout must be greater than BigtableRequestOptions.requestTimeout"
                        + " after Flink truncates it to milliseconds");
        return timeout.toNanos();
    }

    private TupleTypeInfo<Tuple2<T, ConditionalResult>> resultType(DataStream<T> input) {
        return new TupleTypeInfo<>(input.getType(), TypeInformation.of(ConditionalResult.class));
    }
}
