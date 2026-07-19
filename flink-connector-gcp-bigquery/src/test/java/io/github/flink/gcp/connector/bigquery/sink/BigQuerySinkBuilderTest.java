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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.util.InstantiationUtil;

import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BigQuerySinkBuilder}. */
class BigQuerySinkBuilderTest {

    private static final TableDestination DESTINATION =
            TableDestination.of("my-project", "my_dataset", "my_table");

    /** A trivial serializable test serializer. */
    private static class TestSerializer implements BigQueryProtoSerializer<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public Message serialize(String element) {
            return Empty.getDefaultInstance();
        }
    }

    @Test
    void defaultWriteMethodIsStorageApiAtLeastOnce() {
        Sink<String> sink =
                BigQuerySink.<String>builder()
                        .destination(DESTINATION)
                        .serializer(new TestSerializer())
                        .build();

        assertThat(sink).isInstanceOf(BigQueryDefaultStreamSink.class);
    }

    @Test
    void dispatchesToExactlyOnceSink() {
        Sink<String> sink =
                BigQuerySink.<String>builder()
                        .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                        .destination(DESTINATION)
                        .serializer(new TestSerializer())
                        .build();

        assertThat(sink).isInstanceOf(BigQueryExactlyOnceSink.class);
    }

    @Test
    void dispatchesToFileLoadsSink() {
        Sink<String> sink =
                BigQuerySink.<String>builder()
                        .writeMethod(WriteMethod.FILE_LOADS)
                        .destination(DESTINATION)
                        .serializer(new TestSerializer())
                        .build();

        assertThat(sink).isInstanceOf(BigQueryFileLoadsSink.class);
    }

    @Test
    void fixedDestinationIsWrappedAsResolver() {
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destination(DESTINATION)
                                .serializer(new TestSerializer())
                                .build();

        assertThat(sink.getConfig().getDestinationResolver().resolve("any")).isEqualTo(DESTINATION);
    }

    @Test
    void acceptsDestinationResolver() {
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destinationResolver(
                                        element ->
                                                TableDestination.of(
                                                        "my-project", "my_dataset", element))
                                .serializer(new TestSerializer())
                                .build();

        assertThat(sink.getConfig().getDestinationResolver().resolve("events"))
                .isEqualTo(TableDestination.of("my-project", "my_dataset", "events"));
    }

    @Test
    void failsWithoutSerializer() {
        assertThatThrownBy(() -> BigQuerySink.<String>builder().destination(DESTINATION).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("serializer");
    }

    @Test
    void failsWithoutDestination() {
        assertThatThrownBy(
                        () ->
                                BigQuerySink.<String>builder()
                                        .serializer(new TestSerializer())
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("destination");
    }

    @Test
    void failsWhenBothDestinationAndResolverAreSet() {
        assertThatThrownBy(
                        () ->
                                BigQuerySink.<String>builder()
                                        .destination(DESTINATION)
                                        .destinationResolver(element -> DESTINATION)
                                        .serializer(new TestSerializer())
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("mutually exclusive");
    }

    @Test
    void sinkIsJavaSerializable() throws Exception {
        BigQueryDefaultStreamSink<String> sink =
                (BigQueryDefaultStreamSink<String>)
                        BigQuerySink.<String>builder()
                                .destinationResolver(
                                        element ->
                                                TableDestination.of(
                                                        "my-project", "my_dataset", element))
                                .serializer(new TestSerializer())
                                .build();

        BigQueryDefaultStreamSink<String> copy = InstantiationUtil.clone(sink);

        assertThat(copy.getConfig().getDestinationResolver().resolve("events"))
                .isEqualTo(TableDestination.of("my-project", "my_dataset", "events"));
    }
}
