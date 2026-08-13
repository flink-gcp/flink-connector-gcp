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

package io.github.flink.gcp.connector.cloudtasks.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.util.Preconditions;

import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.HttpRequest;
import com.google.cloud.tasks.v2.Task;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.cloudtasks.sink.serializer.CloudTasksSerializationSchema;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.stream.IntStream;

/** Builds HTTP-target tasks from a format-encoded physical row and writable metadata columns. */
@Internal
final class RowDataSerializationSchema implements CloudTasksSerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final SerializationSchema<RowData> physical;
    private final int physicalArity;
    private final boolean hasMetadata;
    private final RowDataToTaskMetadataConverter metadataConverter;

    @Nullable private transient ProjectedRowData projection;

    RowDataSerializationSchema(
            SerializationSchema<RowData> physical,
            int physicalArity,
            WritableMetadata[] metadata,
            TableHttpTarget target) {
        Preconditions.checkArgument(physicalArity >= 0, "physicalArity must not be negative");
        this.physical = Preconditions.checkNotNull(physical, "physical must not be null");
        this.physicalArity = physicalArity;
        this.hasMetadata =
                Preconditions.checkNotNull(metadata, "metadata must not be null").length != 0;
        this.metadataConverter =
                new RowDataToTaskMetadataConverter(physicalArity, metadata, target);
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        physical.open(context);
    }

    @Override
    public Task serialize(RowData element) throws IOException {
        Task.Builder task = metadataConverter.convert(element);
        HttpRequest.Builder request = task.getHttpRequestBuilder();
        HttpMethod method = request.getHttpMethod();

        if (carriesBody(method)) {
            byte[] payload = physical.serialize(hasMetadata ? projected(element) : element);
            if (payload == null) {
                throw new IOException(
                        "The format "
                                + physical.getClass().getName()
                                + " returned null for a row. Flink's SerializationSchema contract"
                                + " has no null in it, so this is a serialization failure rather"
                                + " than the Cloud Tasks skip-record convention.");
            }
            request.setBody(ByteString.copyFrom(payload));
        } else {
            request.clearBody();
        }
        return task.build();
    }

    private RowData projected(RowData element) {
        ProjectedRowData view = projection;
        if (view == null) {
            view = ProjectedRowData.from(IntStream.range(0, physicalArity).toArray());
            projection = view;
        }
        return view.replaceRow(element);
    }

    private static boolean carriesBody(HttpMethod method) {
        return method == HttpMethod.POST || method == HttpMethod.PUT || method == HttpMethod.PATCH;
    }
}
