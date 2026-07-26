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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import org.apache.flink.annotation.Internal;

import com.google.protobuf.Descriptors;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import java.io.IOException;
import java.io.Serializable;

/**
 * Creates {@link RowAppender}s for destinations. The factory is shipped inside the job graph and
 * must be serializable; implementations create all client state at {@link #create} time.
 */
@Internal
public interface RowAppenderFactory extends Serializable {

    /**
     * Creates an appender for the given destination.
     *
     * @param destination the destination table
     * @param rowDescriptor the descriptor of the serialized rows written to that destination
     * @param location the BigQuery location shared by the destinations, or {@code null}
     * @return the appender
     * @throws IOException if the underlying stream writer cannot be created
     */
    RowAppender create(
            TableDestination destination, Descriptors.Descriptor rowDescriptor, String location)
            throws IOException;
}
