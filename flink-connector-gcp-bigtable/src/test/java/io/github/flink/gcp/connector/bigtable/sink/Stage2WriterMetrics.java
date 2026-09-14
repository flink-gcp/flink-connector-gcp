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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import io.github.flink.gcp.connector.bigtable.BigtableMetricNames;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/** Observes the production gauges while forwarding every registration to Flink. */
final class Stage2WriterMetrics {
    private Gauge<?> entries;
    private Gauge<?> bytes;

    WriterInitContext observe(WriterInitContext context) {
        SinkWriterMetricGroup delegate = context.metricGroup();
        SinkWriterMetricGroup metrics =
                (SinkWriterMetricGroup)
                        Proxy.newProxyInstance(
                                SinkWriterMetricGroup.class.getClassLoader(),
                                new Class<?>[] {SinkWriterMetricGroup.class},
                                (proxy, method, args) -> {
                                    Object result = invoke(method, delegate, args);
                                    if (method.getName().equals("gauge") && args.length == 2) {
                                        if (BigtableMetricNames.STAGED_ENTRIES.equals(args[0])) {
                                            entries = (Gauge<?>) args[1];
                                        } else if (BigtableMetricNames.STAGED_BYTES.equals(
                                                args[0])) {
                                            bytes = (Gauge<?>) args[1];
                                        }
                                    }
                                    return result;
                                });
        return (WriterInitContext)
                Proxy.newProxyInstance(
                        WriterInitContext.class.getClassLoader(),
                        new Class<?>[] {WriterInitContext.class},
                        (proxy, method, args) ->
                                method.getName().equals("metricGroup")
                                        ? metrics
                                        : invoke(method, context, args));
    }

    void sample(LocalStagedHarness run, Object writer) {
        if (entries == null || bytes == null) {
            throw new IllegalStateException("Production staged writer did not register its gauges");
        }
        int count = ((Number) entries.getValue()).intValue();
        long size = ((Number) bytes.getValue()).longValue();
        run.writerStaged(writer, count, size);
        run.peakWriterEntries.accumulateAndGet(count, Math::max);
        run.peakWriterBytes.accumulateAndGet(size, Math::max);
    }

    private static Object invoke(Method method, Object delegate, Object[] arguments)
            throws Throwable {
        try {
            return method.invoke(delegate, arguments);
        } catch (InvocationTargetException failure) {
            throw failure.getCause();
        }
    }
}
