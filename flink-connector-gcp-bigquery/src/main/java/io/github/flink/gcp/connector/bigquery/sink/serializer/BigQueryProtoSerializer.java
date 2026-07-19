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

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import java.io.IOException;
import java.io.Serializable;

/**
 * Serializes records into protobuf rows to be written to BigQuery.
 *
 * <p>{@link #getDescriptor(TableDestination)} supplies the protobuf descriptor for a destination
 * <em>before</em> any record is written to it: the sink uses it to derive the BigQuery table schema
 * for table creation ({@code CREATE_IF_NEEDED}), the Storage Write API stream schema, and load-job
 * schemas. The bytes returned by {@link #serialize(Object)} must be valid serialized messages of
 * the descriptor returned for the record's destination.
 *
 * <p>This is deliberately an abstract class rather than a functional interface: implementations are
 * shipped inside the Flink job graph and must be {@link Serializable}, while protobuf {@link
 * Descriptors.Descriptor} instances are <em>not</em> serializable. Implementations must therefore
 * obtain descriptors statically or lazily (for example from generated message classes, or rebuilt
 * from a serialized {@code DescriptorProto}) instead of storing them in instance fields.
 *
 * @param <T> type of the records written by the sink
 */
@PublicEvolving
public abstract class BigQueryProtoSerializer<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Returns the protobuf descriptor describing rows written to the given destination.
     *
     * <p>Invoked when the sink first prepares a destination (table creation, write-stream or
     * load-job schema derivation), not per record. All records serialized for the same destination
     * must conform to the returned descriptor.
     *
     * @param destination the destination table
     * @return the descriptor of the rows written to that destination
     */
    public abstract Descriptors.Descriptor getDescriptor(TableDestination destination);

    /**
     * Serializes a record into protobuf row bytes.
     *
     * <p>For record types that already are protobuf messages this is typically {@code
     * message.toByteString()}.
     *
     * @param element the record
     * @return the serialized protobuf row
     * @throws IOException if the record cannot be serialized
     */
    public abstract ByteString serialize(T element) throws IOException;
}
