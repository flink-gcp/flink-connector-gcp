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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;

import javax.annotation.Nullable;

import java.io.Serializable;

/** Resolves a reader-restored split against current retention. */
@Internal
public interface ChangeStreamRestoreResolver extends Serializable {

    /**
     * Returns the split to read, restarted at the fallback position when the restored one expired.
     *
     * @param split the split the reader was assigned after a restore
     * @param fallback the configured fallback start position, or {@code null} when none was
     *     configured, in which case an expired position fails the job
     * @return the given split, or a copy restarted at the resolved fallback position
     * @throws Exception if retention discovery fails
     */
    ChangeStreamPartitionSplit resolve(
            ChangeStreamPartitionSplit split, @Nullable StartPosition fallback) throws Exception;
}
