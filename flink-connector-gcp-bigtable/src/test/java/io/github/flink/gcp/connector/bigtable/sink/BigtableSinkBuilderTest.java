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
        assertThat(config.getDestinationResolver())
                .isInstanceOfSatisfying(
                        FixedDestinationResolver.class,
                        resolver -> assertThat(resolver.getDestination()).isEqualTo(TABLE));
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
        // Never create, so a sink nobody configured cannot invent a table whose
        // garbage-collection policy somebody then has to fix.
        assertThat(config.getCreateDisposition()).isEqualTo(CreateDisposition.CREATE_NEVER);
        assertThat(config.getTableCreateOptions()).isNull();
    }

    @Test
    void carriesTheCreateDispositionAndItsOptions() {
        TableCreateOptions createOptions =
                TableCreateOptions.builder().columnFamily("cf", GcRule.maxVersions(1)).build();

        BigtableSinkConfig<String> config =
                config(
                        BigtableSink.<String>builder()
                                .table(TABLE)
                                .serializer(SERIALIZER)
                                .createDisposition(CreateDisposition.CREATE_IF_NEEDED)
                                .tableCreateOptions(createOptions)
                                .build());

        assertThat(config.getCreateDisposition()).isEqualTo(CreateDisposition.CREATE_IF_NEEDED);
        assertThat(config.getTableCreateOptions()).isSameAs(createOptions);
    }

    @Test
    void rejectsCreateOptionsBesideCreateNever() {
        TableCreateOptions createOptions = TableCreateOptions.builder().columnFamily("cf").build();
        // The likeliest mistake first: options set with the disposition left at its CREATE_NEVER
        // default — the check must read the final state, not whether the setter was called.
        assertThatThrownBy(
                        () ->
                                BigtableSink.<String>builder()
                                        .table(TABLE)
                                        .serializer(SERIALIZER)
                                        .tableCreateOptions(createOptions)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CREATE_NEVER");
        // And in both explicit setter orders.
        assertThatThrownBy(
                        () ->
                                BigtableSink.<String>builder()
                                        .table(TABLE)
                                        .serializer(SERIALIZER)
                                        .tableCreateOptions(createOptions)
                                        .createDisposition(CreateDisposition.CREATE_NEVER)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CREATE_NEVER");
        assertThatThrownBy(
                        () ->
                                BigtableSink.<String>builder()
                                        .table(TABLE)
                                        .serializer(SERIALIZER)
                                        .createDisposition(CreateDisposition.CREATE_NEVER)
                                        .tableCreateOptions(createOptions)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CREATE_NEVER");
    }

    @Test
    void requiresCreateOptionsBesideCreateIfNeeded() {
        // The issue's central point: a Bigtable table's schema is its column families, so a
        // disposition alone is not enough to create one.
        assertThatThrownBy(
                        () ->
                                BigtableSink.<String>builder()
                                        .table(TABLE)
                                        .serializer(SERIALIZER)
                                        .createDisposition(CreateDisposition.CREATE_IF_NEEDED)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("tableCreateOptions");
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
    void requiresADestinationAndASerializer() {
        assertThatThrownBy(() -> BigtableSink.<String>builder().serializer(SERIALIZER).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("table(...) or destinationResolver(...)");
        assertThatThrownBy(() -> BigtableSink.<String>builder().table(TABLE).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("serializer");
    }

    @Test
    void carriesADestinationResolver() {
        DestinationResolver<String> resolver = (element, context) -> TABLE;

        BigtableSinkConfig<String> config =
                config(
                        BigtableSink.<String>builder()
                                .destinationResolver(resolver)
                                .serializer(SERIALIZER)
                                .build());

        assertThat(config.getDestinationResolver()).isSameAs(resolver);
    }

    @Test
    void letsTheLastOfTableAndResolverWin() {
        // The two setters write one field, as the Pub/Sub builder's pair does, so neither has to
        // be unset before the other can be used.
        DestinationResolver<String> resolver = (element, context) -> TABLE;

        assertThat(
                        config(
                                        BigtableSink.<String>builder()
                                                .table(TABLE)
                                                .destinationResolver(resolver)
                                                .serializer(SERIALIZER)
                                                .build())
                                .getDestinationResolver())
                .isSameAs(resolver);
        assertThat(
                        config(
                                        BigtableSink.<String>builder()
                                                .destinationResolver(resolver)
                                                .table(TABLE)
                                                .serializer(SERIALIZER)
                                                .build())
                                .getDestinationResolver())
                .isInstanceOf(FixedDestinationResolver.class);
    }

    @Test
    void rejectsNullAndBlankSettings() {
        BigtableSinkBuilder<String> builder = BigtableSink.builder();

        assertThatThrownBy(() -> builder.table(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.destinationResolver(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.serializer(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.writerOptions(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.failedMutationHandler(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.appProfileId("  "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.emulatorEndpoint(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.createDisposition(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.tableCreateOptions(null))
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
