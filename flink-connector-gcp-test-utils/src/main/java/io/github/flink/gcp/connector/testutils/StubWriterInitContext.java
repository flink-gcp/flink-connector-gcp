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

package io.github.flink.gcp.connector.testutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.JobInfoImpl;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import java.lang.reflect.Proxy;
import java.util.OptionalLong;

/**
 * The parts of {@link WriterInitContext} a sink reads when creating a writer; everything a sink has
 * no reason to touch is unsupported, so a new dependency on the context shows up as a failing test
 * rather than as a silent null.
 *
 * <p>The metric group is a no-op proxy rather than a real group: a test can then assert by identity
 * that the group reached whatever it was handed to, without this class implementing the interface's
 * many methods. It is a field, so repeated calls return the same instance.
 */
@Internal
public final class StubWriterInitContext implements WriterInitContext {

    private final int subtaskIndex;
    private final int parallelism;
    private final FakeMailboxExecutor mailboxExecutor = new FakeMailboxExecutor();

    private final SinkWriterMetricGroup metricGroup =
            (SinkWriterMetricGroup)
                    Proxy.newProxyInstance(
                            SinkWriterMetricGroup.class.getClassLoader(),
                            new Class<?>[] {SinkWriterMetricGroup.class},
                            (proxy, method, args) -> null);

    /**
     * Creates a context for a single-subtask writer.
     *
     * @param subtaskIndex the subtask index the context reports
     */
    public StubWriterInitContext(int subtaskIndex) {
        this(subtaskIndex, subtaskIndex + 1);
    }

    /**
     * Creates a context.
     *
     * @param subtaskIndex the subtask index the context reports
     * @param parallelism the parallelism the context reports
     */
    public StubWriterInitContext(int subtaskIndex, int parallelism) {
        this.subtaskIndex = subtaskIndex;
        this.parallelism = parallelism;
    }

    /** Returns the mailbox the context hands to writers, so a test can run its mails. */
    public FakeMailboxExecutor getFakeMailboxExecutor() {
        return mailboxExecutor;
    }

    @Override
    public TaskInfo getTaskInfo() {
        return new TaskInfoImpl("task", parallelism, subtaskIndex, parallelism, 0);
    }

    @Override
    public JobInfo getJobInfo() {
        return new JobInfoImpl(new JobID(), "job");
    }

    @Override
    public SinkWriterMetricGroup metricGroup() {
        return metricGroup;
    }

    @Override
    public OptionalLong getRestoredCheckpointId() {
        return OptionalLong.empty();
    }

    @Override
    public ProcessingTimeService getProcessingTimeService() {
        return null;
    }

    @Override
    public boolean isObjectReuseEnabled() {
        return false;
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MailboxExecutor getMailboxExecutor() {
        return mailboxExecutor;
    }

    @Override
    public SerializationSchema.InitializationContext asSerializationSchemaInitializationContext() {
        return new SerializationSchema.InitializationContext() {
            @Override
            public MetricGroup getMetricGroup() {
                return metricGroup;
            }

            @Override
            public UserCodeClassLoader getUserCodeClassLoader() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public <IN> TypeSerializer<IN> createInputSerializer() {
        throw new UnsupportedOperationException();
    }
}
