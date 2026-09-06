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

package io.github.flink.gcp.connector.cloudtasks.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.CloudTasksStagedWriter;

import java.io.IOException;

/**
 * Internal construction seam for the staged writer and its committable serializer. This class is
 * deliberately abstract until the production committer and graph validation exist; the public
 * builder continues to construct the eager sink.
 *
 * @param <T> input record type
 */
@Internal
public abstract class CloudTasksStagedCreateTaskSink<T>
        implements CrossVersionSink<T>, SupportsCommitter<CloudTasksCommittable> {
    private static final long serialVersionUID = 1L;
    private final CloudTasksSinkConfig<T> config;
    private final CloudTasksStagingConfig staging;

    CloudTasksStagedCreateTaskSink(
            CloudTasksSinkConfig<T> config, CloudTasksStagingConfig staging) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.staging = Preconditions.checkNotNull(staging, "staging");
        validate();
    }

    private void validate() {
        staging.validate();
        Preconditions.checkArgument(
                config.getFailedTaskHandler() == FailureHandler.failJob(),
                "Cloud Tasks staging requires FailureHandler.failJob()");
    }

    @Override
    public final CommittingSinkWriter<T, CloudTasksCommittable> createWriter(
            WriterInitContext context) throws IOException {
        validate();
        try {
            config.getSerializer().open(context.asSerializationSchemaInitializationContext());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while opening the Cloud Tasks staging serializer.");
        } catch (Exception e) {
            throw new IOException("Failed to open the Cloud Tasks staging serializer.");
        }
        return createStagedWriter(config, staging, context);
    }

    /** Constructs only the writer; overrides can inject a clock and random source in tests. */
    protected CloudTasksStagedWriter<T> createStagedWriter(
            CloudTasksSinkConfig<T> sinkConfig,
            CloudTasksStagingConfig stagingConfig,
            WriterInitContext context) {
        return new CloudTasksStagedWriter<>(sinkConfig, stagingConfig, context.metricGroup());
    }

    @Override
    public final SimpleVersionedSerializer<CloudTasksCommittable> getCommittableSerializer() {
        return new CloudTasksCommittableSerializer();
    }
}
