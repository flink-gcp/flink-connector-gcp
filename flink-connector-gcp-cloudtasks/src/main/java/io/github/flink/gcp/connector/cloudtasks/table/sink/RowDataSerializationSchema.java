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

package io.github.flink.gcp.connector.cloudtasks.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.util.Preconditions;

import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.Task;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.cloudtasks.sink.serializer.CloudTasksSerializationSchema;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.stream.IntStream;

/** Builds target tasks from a format-encoded physical row and writable metadata columns. */
@Internal
final class RowDataSerializationSchema implements CloudTasksSerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final SerializationSchema<RowData> physical;
    private final int physicalArity;
    private final boolean hasMetadata;
    private final RowDataToTaskConverter taskConverter;
    @Nullable private final String bodyContentType;

    @Nullable private transient ProjectedRowData projection;

    RowDataSerializationSchema(
            SerializationSchema<RowData> physical,
            int physicalArity,
            WritableMetadata[] metadata,
            TargetSpec target) {
        Preconditions.checkArgument(physicalArity >= 0, "physicalArity must not be negative");
        this.physical = Preconditions.checkNotNull(physical, "physical must not be null");
        this.physicalArity = physicalArity;
        this.hasMetadata =
                Preconditions.checkNotNull(metadata, "metadata must not be null").length != 0;
        this.taskConverter = target.converter(physicalArity, metadata);
        this.bodyContentType = target.getBodyContentType();
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        physical.open(context);
    }

    @Override
    public Task serialize(RowData element) throws IOException {
        Task.Builder task = taskConverter.convert(element);

        if (carriesBody(task)) {
            byte[] payload = physical.serialize(hasMetadata ? projected(element) : element);
            if (payload == null) {
                throw new IOException(
                        "The format "
                                + physical.getClass().getName()
                                + " returned null for a row. Flink's SerializationSchema contract"
                                + " has no null in it, so this is a serialization failure rather"
                                + " than the Cloud Tasks skip-record convention.");
            }
            setBody(task, ByteString.copyFrom(payload));
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

    private static boolean carriesBody(Task.Builder task) {
        switch (task.getMessageTypeCase()) {
            case HTTP_REQUEST:
                HttpMethod httpMethod = task.getHttpRequest().getHttpMethod();
                return httpMethod == HttpMethod.POST
                        || httpMethod == HttpMethod.PUT
                        || httpMethod == HttpMethod.PATCH;
            case APP_ENGINE_HTTP_REQUEST:
                HttpMethod appEngineMethod = task.getAppEngineHttpRequest().getHttpMethod();
                return appEngineMethod == HttpMethod.POST || appEngineMethod == HttpMethod.PUT;
            default:
                throw new IllegalStateException(
                        "The Cloud Tasks table converter did not select a request target.");
        }
    }

    private void setBody(Task.Builder task, ByteString payload) {
        switch (task.getMessageTypeCase()) {
            case HTTP_REQUEST:
                if (bodyContentType != null) {
                    task.getHttpRequestBuilder().putHeaders("Content-Type", bodyContentType);
                }
                task.getHttpRequestBuilder().setBody(payload);
                return;
            case APP_ENGINE_HTTP_REQUEST:
                if (bodyContentType != null) {
                    task.getAppEngineHttpRequestBuilder()
                            .putHeaders("Content-Type", bodyContentType);
                }
                task.getAppEngineHttpRequestBuilder().setBody(payload);
                return;
            default:
                throw new IllegalStateException(
                        "The Cloud Tasks table converter did not select a request target.");
        }
    }
}
