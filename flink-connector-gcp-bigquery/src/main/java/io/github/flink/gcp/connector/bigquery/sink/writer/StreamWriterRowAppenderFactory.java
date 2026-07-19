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

package io.github.flink.gcp.connector.bigquery.sink.writer;

import org.apache.flink.annotation.Internal;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchemaConverter;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.protobuf.Descriptors;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import java.io.IOException;

/**
 * Default {@link RowAppenderFactory} backed by Storage Write API {@link StreamWriter}s on the
 * destination's default write stream.
 *
 * <p>Connection multiplexing is delegated to the client's connection pool ({@code
 * setEnableConnectionPool(true)}): the pool is JVM-static per (location, credentials), shares
 * connections across destination tables, scales with load, and transparently reconnects on
 * server-side idle disconnects. One lightweight {@code StreamWriter} exists per destination.
 */
@Internal
public class StreamWriterRowAppenderFactory implements RowAppenderFactory {

    private static final long serialVersionUID = 1L;

    private static final String TRACE_ID = "flink-connector-gcp-bigquery";

    @Override
    public RowAppender create(
            TableDestination destination, Descriptors.Descriptor rowDescriptor, String location)
            throws IOException {
        StreamWriter.Builder builder =
                StreamWriter.newBuilder(destination.toTablePath() + "/_default")
                        .setWriterSchema(ProtoSchemaConverter.convert(rowDescriptor))
                        .setEnableConnectionPool(true)
                        .setTraceId(TRACE_ID);
        if (location != null) {
            builder.setLocation(location);
        }
        StreamWriter streamWriter = builder.build();
        return new StreamWriterRowAppender(streamWriter);
    }

    private static final class StreamWriterRowAppender implements RowAppender {

        private final StreamWriter streamWriter;

        StreamWriterRowAppender(StreamWriter streamWriter) {
            this.streamWriter = streamWriter;
        }

        @Override
        public ApiFuture<AppendRowsResponse> append(ProtoRows rows) {
            return streamWriter.append(rows);
        }

        @Override
        public void close() {
            streamWriter.close();
        }
    }
}
