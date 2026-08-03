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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.api.connector.sink2.Sink;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.serializer.BigtableSerializationSchema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BigtableSinkBuilder}. */
class BigtableSinkBuilderTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");

    private static final BigtableSerializationSchema<String> SERIALIZER =
            (element, context) -> RowMutationEntry.create(element).setCell("cf", "q", element);

    @Test
    void carriesEverySettingIntoTheSinkConfig() {
        FailureHandler<FailedMutation> handler = mutation -> {};
        BigtableWriterOptions writerOptions =
                BigtableWriterOptions.builder().maxInFlightMutations(5).build();

        Sink<String> sink =
                BigtableSink.<String>builder()
                        .table(TABLE)
                        .serializer(SERIALIZER)
                        .appProfileId("batch-profile")
                        .writerOptions(writerOptions)
                        .failedMutationHandler(handler)
                        .emulatorEndpoint("localhost:8086")
                        .build();

        BigtableSinkConfig<String> config = config(sink);
        assertThat(config.getDestination()).isEqualTo(TABLE);
        assertThat(config.getSerializer()).isSameAs(SERIALIZER);
        assertThat(config.getAppProfileId()).isEqualTo("batch-profile");
        assertThat(config.getWriterOptions()).isSameAs(writerOptions);
        assertThat(config.getFailedMutationHandler()).isSameAs(handler);
        assertThat(config.getEmulatorEndpoint())
                .isEqualTo(EmulatorEndpoint.parse("localhost:8086"));
    }

    @Test
    void defaultsTheOptionalSettings() {
        BigtableSinkConfig<String> config =
                config(BigtableSink.<String>builder().table(TABLE).serializer(SERIALIZER).build());

        assertThat(config.getAppProfileId()).isNull();
        assertThat(config.getEmulatorEndpoint()).isNull();
        assertThat(config.getWriterOptions()).isEqualTo(BigtableWriterOptions.defaults());
        // Fail the job, so a mutation is never dropped by a sink nobody configured.
        assertThat(config.getFailedMutationHandler()).isSameAs(FailureHandler.failJob());
    }

    @Test
    void acceptsACrossConnectorFailureHandlerWithoutACast() {
        FailureHandler<io.github.flink.gcp.connector.base.failure.FailedElement> handler =
                element -> {};

        BigtableSinkConfig<String> config =
                config(
                        BigtableSink.<String>builder()
                                .table(TABLE)
                                .serializer(SERIALIZER)
                                .failedMutationHandler(handler)
                                .build());

        assertThat(config.getFailedMutationHandler()).isSameAs(handler);
    }

    @Test
    void requiresATableAndASerializer() {
        assertThatThrownBy(() -> BigtableSink.<String>builder().serializer(SERIALIZER).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("table");
        assertThatThrownBy(() -> BigtableSink.<String>builder().table(TABLE).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("serializer");
    }

    @Test
    void rejectsNullAndBlankSettings() {
        BigtableSinkBuilder<String> builder = BigtableSink.builder();

        assertThatThrownBy(() -> builder.table(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.serializer(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.writerOptions(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.failedMutationHandler(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.appProfileId("  "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.emulatorEndpoint(null))
                .isInstanceOf(NullPointerException.class);
        // Parsed at the setter, so a typo fails on the client rather than when the writer is
        // created; the full parse table is EmulatorEndpointTest's.
        assertThatThrownBy(() -> builder.emulatorEndpoint("localhost8086"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("emulatorEndpoint must be host:port, was 'localhost8086'");
    }

    @SuppressWarnings("unchecked")
    private static BigtableSinkConfig<String> config(Sink<String> sink) {
        assertThat(sink).isInstanceOf(BigtableMutateRowsSink.class);
        return ((BigtableMutateRowsSink<String>) sink).getConfig();
    }
}
