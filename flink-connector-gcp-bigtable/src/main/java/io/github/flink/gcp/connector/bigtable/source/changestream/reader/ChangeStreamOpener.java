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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;

/** Serializable seam that opens one {@code ReadChangeStream} RPC. */
@Internal
public interface ChangeStreamOpener extends Serializable, AutoCloseable {

    ChangeStream open(
            TableDestination table, ChangeStreamPartitionSplit split, @Nullable Instant endTime)
            throws IOException;

    @Override
    void close() throws IOException;
}
