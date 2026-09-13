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

import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;

import com.google.bigtable.v2.CheckAndMutateRowRequest;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.LinkedHashSet;
import java.util.Set;

/** Decorates the real Flink committer without copying its checkpoint or retry implementation. */
final class Stage2NotificationOperatorFactory
        extends AbstractStreamOperatorFactory<CommittableMessage<CheckAndMutateRowRequest>>
        implements OneInputStreamOperatorFactory<
                CommittableMessage<CheckAndMutateRowRequest>,
                CommittableMessage<CheckAndMutateRowRequest>> {
    private static final long serialVersionUID = 1L;
    private final CommitterOperatorFactory<CheckAndMutateRowRequest> delegate;
    private final String runId;

    private Stage2NotificationOperatorFactory(
            CommitterOperatorFactory<CheckAndMutateRowRequest> delegate, String runId) {
        this.delegate = delegate;
        this.runId = runId;
        setChainingStrategy(delegate.getChainingStrategy());
    }

    @SuppressWarnings("unchecked")
    static void install(StreamGraph graph, String runId) throws ReflectiveOperationException {
        int found = 0;
        // Flink 1.20 has no setter. Replace only the test job's generated committer factory.
        var factoryField =
                org.apache.flink.streaming.api.graph.StreamNode.class.getDeclaredField(
                        "operatorFactory");
        factoryField.setAccessible(true);
        for (var node : graph.getStreamNodes()) {
            if (node.getOperatorFactory() instanceof CommitterOperatorFactory) {
                factoryField.set(
                        node,
                        new Stage2NotificationOperatorFactory(
                                (CommitterOperatorFactory<CheckAndMutateRowRequest>)
                                        node.getOperatorFactory(),
                                runId));
                found++;
            }
        }
        if (found != 1) {
            throw new IllegalStateException(
                    "Expected exactly one Stage 2 committer factory, found " + found);
        }
    }

    @Override
    public <T extends StreamOperator<CommittableMessage<CheckAndMutateRowRequest>>>
            T createStreamOperator(
                    StreamOperatorParameters<CommittableMessage<CheckAndMutateRowRequest>>
                            parameters) {
        delegate.setProcessingTimeService(processingTimeService);
        Stage2Harness run = (Stage2Harness) LocalStagedHarness.run(runId);
        @SuppressWarnings("unchecked")
        T observed = (T) observe(delegate.createStreamOperator(parameters), run.notifications);
        return observed;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return delegate.getStreamOperatorClass(classLoader);
    }

    static StreamOperator<?> observe(
            StreamOperator<?> delegate, Stage2NotificationProgress progress) {
        Set<Class<?>> interfaces = new LinkedHashSet<>();
        for (Class<?> type = delegate.getClass(); type != null; type = type.getSuperclass()) {
            interfaces.addAll(java.util.Arrays.asList(type.getInterfaces()));
        }
        return (StreamOperator<?>)
                Proxy.newProxyInstance(
                        delegate.getClass().getClassLoader(),
                        interfaces.toArray(new Class<?>[0]),
                        (proxy, method, arguments) -> {
                            boolean notification =
                                    method.getName().equals("notifyCheckpointComplete")
                                            && method.getParameterCount() == 1;
                            boolean successful = false;
                            if (notification) {
                                progress.started((Long) arguments[0], System.nanoTime());
                            }
                            try {
                                Object result = method.invoke(delegate, arguments);
                                successful = true;
                                return result;
                            } catch (InvocationTargetException failure) {
                                throw failure.getCause();
                            } finally {
                                if (notification) {
                                    progress.finished(successful, System.nanoTime());
                                }
                            }
                        });
    }
}
