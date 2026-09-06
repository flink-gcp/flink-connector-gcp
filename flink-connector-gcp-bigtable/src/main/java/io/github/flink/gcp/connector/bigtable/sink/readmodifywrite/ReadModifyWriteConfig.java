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

package io.github.flink.gcp.connector.bigtable.sink.readmodifywrite;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.sink.DestinationResolver;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRow;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.RowRequest;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.RowRequestSerializer;

import java.io.IOException;
import java.io.Serializable;

/** Immutable configuration shared by the read-modify-write sink and async helper. */
@Internal
final class ReadModifyWriteConfig<T> implements Serializable {
    private static final long serialVersionUID = 1L;
    final DestinationResolver<? super T> destinationResolver;
    final ReadModifyWriteSerializationSchema<? super T> serializer;
    final String appProfileId;
    final BigtableRequestOptions requestOptions;
    final String serviceAccountKeyFile;
    final EmulatorEndpoint emulatorEndpoint;

    ReadModifyWriteConfig(
            DestinationResolver<? super T> resolver,
            ReadModifyWriteSerializationSchema<? super T> serializer,
            String appProfileId,
            BigtableRequestOptions requestOptions,
            String serviceAccountKeyFile,
            EmulatorEndpoint emulatorEndpoint) {
        Preconditions.checkState(
                serviceAccountKeyFile == null || emulatorEndpoint == null,
                "serviceAccountKeyFile(...) cannot be combined with emulatorEndpoint(...): an"
                        + " emulator uses a plaintext channel with no credentials. Remove one of"
                        + " the two settings.");
        this.destinationResolver =
                Preconditions.checkNotNull(
                        resolver,
                        "A destination is required: set table(...) or destinationResolver(...).");
        this.serializer = Preconditions.checkNotNull(serializer, "serializer must not be null");
        this.appProfileId = appProfileId;
        this.requestOptions = requestOptions;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    RowRequestSerializer<T> sinkSerializer() {
        return new Adapter<>(serializer);
    }

    private static final class Adapter<T> implements RowRequestSerializer<T> {
        private static final long serialVersionUID = 1L;
        private final ReadModifyWriteSerializationSchema<? super T> serializer;

        private Adapter(ReadModifyWriteSerializationSchema<? super T> serializer) {
            this.serializer = serializer;
        }

        @Override
        public void open(SerializationSchema.InitializationContext context) throws Exception {
            serializer.open(context);
        }

        @Override
        public RowRequest<BigtableRow> serialize(T input, SinkWriter.Context context)
                throws IOException {
            ReadModifyWriteRequest request = serializer.serialize(input, context);
            return request == null ? null : request.toRequest();
        }
    }
}
