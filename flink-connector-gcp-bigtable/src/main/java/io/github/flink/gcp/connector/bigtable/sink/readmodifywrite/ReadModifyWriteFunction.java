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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRow;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.BigtableRequestFunction;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.RowRequest;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowClientFactory;

/** Correlates a response with the exact request and destination used by the runtime. */
@Internal
final class ReadModifyWriteFunction<T>
        extends BigtableRequestFunction<T, BigtableRow, Tuple2<T, ReadModifyWriteResult>> {
    private static final long serialVersionUID = 1L;
    private final ReadModifyWriteConfig<T> config;

    ReadModifyWriteFunction(ReadModifyWriteConfig<T> config) {
        super(
                config.appProfileId,
                config.requestOptions,
                config.serviceAccountKeyFile,
                config.emulatorEndpoint);
        this.config = config;
    }

    ReadModifyWriteFunction(ReadModifyWriteConfig<T> config, SingleRowClientFactory factory) {
        super(factory, config.requestOptions);
        this.config = config;
    }

    @Override
    public void open(OpenContext context) throws Exception {
        super.open(context);
        try {
            config.serializer.open(
                    new SerializationSchema.InitializationContext() {
                        @Override
                        public MetricGroup getMetricGroup() {
                            return getRuntimeContext().getMetricGroup();
                        }

                        @Override
                        public UserCodeClassLoader getUserCodeClassLoader() {
                            return new UserCodeClassLoader() {
                                @Override
                                public ClassLoader asClassLoader() {
                                    return getRuntimeContext().getUserCodeClassLoader();
                                }

                                @Override
                                public void registerReleaseHookIfAbsent(
                                        String name, Runnable hook) {
                                    getRuntimeContext()
                                            .registerUserCodeClassLoaderReleaseHookIfAbsent(
                                                    name, hook);
                                }
                            };
                        }
                    });
        } catch (Exception e) {
            try {
                super.close();
            } catch (Exception closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    @Override
    protected TableDestination destination(T input) {
        return config.destinationResolver.resolve(input, null);
    }

    @Override
    protected RowRequest<BigtableRow> request(T input) throws Exception {
        ReadModifyWriteRequest request = config.serializer.serialize(input, null);
        return request == null ? null : request.toRequest();
    }

    @Override
    protected Tuple2<T, ReadModifyWriteResult> result(
            T input,
            BigtableRow answer,
            TableDestination destination,
            RowRequest<BigtableRow> request) {
        return Tuple2.of(input, new ReadModifyWriteResult(destination, answer));
    }
}
