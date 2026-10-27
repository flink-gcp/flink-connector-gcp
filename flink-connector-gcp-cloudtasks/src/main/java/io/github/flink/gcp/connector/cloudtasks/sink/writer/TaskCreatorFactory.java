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

package io.github.flink.gcp.connector.cloudtasks.sink.writer;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.base.metrics.ClientMetrics;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/**
 * Creates the writer's {@link TaskCreator}.
 *
 * <p>Serializable because the factory is shipped in the job graph; implementations must create all
 * client state at {@link #create(ClientMetrics)} time, not at construction time.
 */
@Internal
public interface TaskCreatorFactory extends Serializable {

    /**
     * Creates the task creator.
     *
     * @param clientMetrics the client metrics the creator's client records into, or {@code null} to
     *     export nothing to Flink. A creator that is returned owns them and closes them after its
     *     client; they hold no resource, so a failed creation leaves nothing of theirs to release.
     * @return the creator
     * @throws IOException if the client cannot be created
     */
    TaskCreator create(@Nullable ClientMetrics clientMetrics) throws IOException;
}
