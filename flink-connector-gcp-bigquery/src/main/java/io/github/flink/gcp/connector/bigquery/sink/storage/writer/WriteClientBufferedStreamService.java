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
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.FlushRowsRequest;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchemaConverter;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Int64Value;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Production {@link BufferedStreamService} over one {@link BigQueryWriteClient}.
 *
 * <p>Unlike the default-stream path, appenders here never enable the SDK connection pool: the pool
 * multiplexes default-stream writers only, so each buffered stream gets a dedicated {@link
 * StreamWriter} bound to the shared client.
 */
@Internal
public final class WriteClientBufferedStreamService implements BufferedStreamService {

    private final BigQueryWriteClient client;
    @Nullable private final String location;

    /**
     * Creates a service with a default client.
     *
     * @param location the BigQuery location routing hint for appends, or {@code null}
     * @throws IOException if the client cannot be created
     */
    public WriteClientBufferedStreamService(@Nullable String location) throws IOException {
        this(BigQueryWriteClient.create(), location);
    }

    /**
     * Creates a service over the given client (emulator or test injection). The service takes
     * ownership of the client.
     *
     * @param client the write client
     * @param location the BigQuery location routing hint for appends, or {@code null}
     */
    @VisibleForTesting
    public WriteClientBufferedStreamService(BigQueryWriteClient client, @Nullable String location) {
        this.client = client;
        this.location = location;
    }

    @Override
    public String createBufferedStream(TableDestination destination) throws IOException {
        return client.createWriteStream(
                        CreateWriteStreamRequest.newBuilder()
                                .setParent(destination.toTablePath())
                                .setWriteStream(
                                        WriteStream.newBuilder()
                                                .setType(WriteStream.Type.BUFFERED)
                                                .build())
                                .build())
                .getName();
    }

    @Override
    public OffsetRowAppender openAppender(String streamName, Descriptors.Descriptor rowDescriptor)
            throws IOException {
        StreamWriter.Builder builder =
                StreamWriter.newBuilder(streamName, client)
                        .setWriterSchema(ProtoSchemaConverter.convert(rowDescriptor))
                        .setRetrySettings(StreamWriterRowAppenderFactory.RETRY_SETTINGS)
                        .setTraceId(StreamWriterRowAppenderFactory.TRACE_ID);
        if (location != null) {
            builder.setLocation(location);
        }
        StreamWriter streamWriter = builder.build();
        return new OffsetRowAppender() {
            @Override
            public ApiFuture<AppendRowsResponse> append(ProtoRows rows, long offset) {
                return streamWriter.append(rows, offset);
            }

            @Override
            public void close() {
                streamWriter.close();
            }
        };
    }

    @Override
    public long flushRows(String streamName, long offset) throws IOException {
        return client.flushRows(
                        FlushRowsRequest.newBuilder()
                                .setWriteStream(streamName)
                                .setOffset(Int64Value.of(offset))
                                .build())
                .getOffset();
    }

    @Override
    public void close() {
        client.close();
    }
}
