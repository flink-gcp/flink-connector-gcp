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

package io.github.flink.gcp.connector.bigquery.sink.serializer;

import org.apache.flink.annotation.PublicEvolving;

import com.google.protobuf.Message;

import java.io.IOException;
import java.io.Serializable;

/**
 * Serializes records into protobuf messages to be written to BigQuery.
 *
 * <p>The message's {@link com.google.protobuf.Descriptors.Descriptor} is used to derive the
 * BigQuery table schema of the destination (for table creation and for the Storage Write API stream
 * schema), so all messages produced for the same destination must share one descriptor.
 *
 * @param <T> type of the records written by the sink
 */
@PublicEvolving
@FunctionalInterface
public interface BigQueryProtoSerializer<T> extends Serializable {

    /**
     * Serializes a record into a protobuf message.
     *
     * @param element the record
     * @return the protobuf message to append to BigQuery
     * @throws IOException if the record cannot be serialized
     */
    Message serialize(T element) throws IOException;
}
