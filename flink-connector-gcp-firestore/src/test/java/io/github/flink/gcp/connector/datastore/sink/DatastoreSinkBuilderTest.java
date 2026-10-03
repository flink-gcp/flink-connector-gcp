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

package io.github.flink.gcp.connector.datastore.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.datastore.Key;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.sink.serializer.DatastoreMutationSerializationSchema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DatastoreSinkBuilderTest {

    private static final DatabaseDestination DATABASE = DatabaseDestination.of("p");

    private static final DatastoreMutationSerializationSchema<String> SERIALIZER =
            (element, context) ->
                    DatastoreMutation.delete(Key.newBuilder("p", "K", element).build());

    @Test
    void buildsASinkFromTheTwoRequiredOptions() {
        Sink<String> sink =
                DatastoreSink.<String>builder().database(DATABASE).serializer(SERIALIZER).build();

        DatastoreSinkConfig<String> config = ((DatastoreCommitSink<String>) sink).getConfig();
        assertThat(config.getDatabase()).isEqualTo(DATABASE);
        assertThat(config.getSerializer()).isSameAs(SERIALIZER);
        assertThat(config.getWriterOptions()).isEqualTo(DatastoreWriterOptions.defaults());
        assertThat(config.getFailedMutationHandler().toString())
                .isEqualTo(FailureHandler.failJob().toString());
        assertThat(config.getServiceAccountKeyFile()).isNull();
        assertThat(config.getEmulatorEndpoint()).isNull();
    }

    @Test
    void carriesEveryOptionItWasGivenThroughJobSubmission() throws Exception {
        DatastoreWriterOptions options =
                DatastoreWriterOptions.builder().maxBatchMutations(3).build();
        Sink<String> sink =
                DatastoreSink.<String>builder()
                        .database(DATABASE)
                        .serializer(SERIALIZER)
                        .writerOptions(options)
                        .failedMutationHandler(FailureHandler.logAndDrop())
                        .serviceAccountKeyFile("/keys/sa.json")
                        .build();

        DatastoreSinkConfig<String> config =
                ((DatastoreCommitSink<String>) InstantiationUtil.clone(sink)).getConfig();
        assertThat(config.getWriterOptions()).isEqualTo(options);
        assertThat(config.getServiceAccountKeyFile()).isEqualTo("/keys/sa.json");
        assertThat(config.getFailedMutationHandler().toString())
                .isEqualTo(FailureHandler.logAndDrop().toString());

        Sink<String> emulated =
                DatastoreSink.<String>builder()
                        .database(DATABASE)
                        .serializer(SERIALIZER)
                        .emulatorEndpoint("localhost:8081")
                        .build();
        assertThat(((DatastoreCommitSink<String>) emulated).getConfig().getEmulatorEndpoint())
                .isNotNull();
    }

    @Test
    void namesTheMissingOptionAndHowToSetIt() {
        assertThatThrownBy(() -> DatastoreSink.<String>builder().serializer(SERIALIZER).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("database(...)");
        assertThatThrownBy(() -> DatastoreSink.<String>builder().database(DATABASE).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("serializer(...)");
    }

    @Test
    void refusesConflictingAndMalformedOptions() {
        assertThatThrownBy(
                        () ->
                                DatastoreSink.<String>builder()
                                        .database(DATABASE)
                                        .serializer(SERIALIZER)
                                        .serviceAccountKeyFile("/keys/sa.json")
                                        .emulatorEndpoint("localhost:8081")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot be combined");
        assertThatThrownBy(() -> DatastoreSink.<String>builder().emulatorEndpoint("nohost"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("emulatorEndpoint must be host:port, was 'nohost'");
        assertThatThrownBy(() -> DatastoreSink.<String>builder().serviceAccountKeyFile(" "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DatastoreSink.<String>builder().database(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> DatastoreSink.<String>builder().failedMutationHandler(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> DatastoreSink.<String>builder().writerOptions(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void acceptsASerializerOfASupertype() {
        DatastoreMutationSerializationSchema<Object> general =
                (element, context) ->
                        DatastoreMutation.delete(Key.newBuilder("p", "K", "x").build());

        assertThat(DatastoreSink.<String>builder().database(DATABASE).serializer(general).build())
                .isNotNull();
    }
}
