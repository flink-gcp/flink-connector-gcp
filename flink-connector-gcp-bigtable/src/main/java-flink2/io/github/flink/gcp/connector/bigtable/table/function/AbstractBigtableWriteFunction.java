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

package io.github.flink.gcp.connector.bigtable.table.function;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;

import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.SingleRowClientFactory;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Adapts Flink's SQL completion future to the existing bounded request operator runtime. */
@Internal
abstract class AbstractBigtableWriteFunction<T> extends AsyncScalarFunction
        implements SpecializedFunction {
    private static final long serialVersionUID = 1L;
    private final boolean conditional;
    private final SqlWriteTemplate template;
    private transient SqlWriteRuntime runtime;

    AbstractBigtableWriteFunction(boolean conditional, SqlWriteTemplate template) {
        this.conditional = conditional;
        this.template = template;
    }

    abstract AbstractBigtableWriteFunction<T> specialized(SqlWriteTemplate compiled);

    @Override
    public final UserDefinedFunction specialize(SpecializedContext context) {
        String name =
                context.getCallContext()
                        .getArgumentValue(0, String.class)
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "A literal Bigtable settings name is required."));
        return specialized(
                SqlWriteTemplate.compile(
                        name,
                        conditional,
                        context.getConfiguration(),
                        context.getCallContext().getArgumentDataTypes()));
    }

    @Override
    public final TypeInference getTypeInference(DataTypeFactory factory) {
        DataType output =
                conditional
                        ? DataTypes.BOOLEAN()
                        : DataTypes.ROW(
                                DataTypes.FIELD("row_key", DataTypes.BYTES()),
                                DataTypes.FIELD(
                                        "cells",
                                        DataTypes.ARRAY(
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "family", DataTypes.STRING()),
                                                        DataTypes.FIELD(
                                                                "qualifier", DataTypes.BYTES()),
                                                        DataTypes.FIELD("value", DataTypes.BYTES()),
                                                        DataTypes.FIELD(
                                                                "timestamp_micros",
                                                                DataTypes.BIGINT()),
                                                        DataTypes.FIELD(
                                                                "value_int64",
                                                                DataTypes.BIGINT())))));
        return TypeInference.newBuilder()
                .inputTypeStrategy(new SqlWriteInputStrategy())
                .outputTypeStrategy(call -> Optional.of(output))
                .build();
    }

    @Override
    public final boolean isDeterministic() {
        return false;
    }

    @Override
    public final boolean supportsConstantFolding() {
        return false;
    }

    @Override
    public final void open(FunctionContext context) throws Exception {
        openRuntime(context.getMetricGroup(), null);
    }

    final void openRuntime(MetricGroup group, SingleRowClientFactory factory) throws Exception {
        if (template == null) {
            throw new IllegalStateException(
                    "Bigtable SQL write function must be specialized by the planner.");
        }
        runtime =
                factory == null
                        ? new SqlWriteRuntime(template)
                        : new SqlWriteRuntime(template, factory);
        try {
            runtime.openWithMetrics(group.addGroup("bigtableSql").addGroup(template.name));
        } catch (Exception failure) {
            try {
                runtime.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            runtime = null;
            throw failure;
        }
    }

    /**
     * Evaluates one configured write asynchronously.
     *
     * @param future the result, completed exceptionally on any invalid input or request failure
     * @param rowKey the row key in its planner-selected internal representation
     * @param values operands in their planner-selected internal representations
     */
    final void evaluate(CompletableFuture<T> future, Object rowKey, Object... values) {
        if (runtime == null) {
            future.completeExceptionally(
                    new IllegalStateException("Bigtable SQL function is not open."));
            return;
        }
        GenericRowData input = new GenericRowData(values.length + 1);
        input.setField(0, rowKey);
        for (int i = 0; i < values.length; i++) {
            input.setField(i + 1, values[i]);
        }
        try {
            runtime.asyncInvoke(
                    input,
                    new ResultFuture<Object>() {
                        @Override
                        @SuppressWarnings("unchecked")
                        public void complete(Collection<Object> results) {
                            if (results.size() != 1) {
                                future.completeExceptionally(
                                        new IllegalStateException(
                                                "A Bigtable SQL write must return one result."));
                            } else {
                                future.complete((T) results.iterator().next());
                            }
                        }

                        @Override
                        public void complete(CollectionSupplier<Object> supplier) {
                            try {
                                complete(supplier.get());
                            } catch (Exception failure) {
                                completeExceptionally(failure);
                            }
                        }

                        @Override
                        public void completeExceptionally(Throwable failure) {
                            future.completeExceptionally(failure);
                        }
                    });
        } catch (Exception failure) {
            future.completeExceptionally(failure);
        }
    }

    @Override
    public final void close() throws Exception {
        SqlWriteRuntime closing = runtime;
        runtime = null;
        if (closing != null) {
            closing.close();
        }
    }
}
