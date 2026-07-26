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

package io.github.flink.gcp.connector.pubsub.table.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import io.github.flink.gcp.connector.pubsub.source.DeserializationFailurePolicy;
import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link PubSubDynamicSource}.
 *
 * <p>Built directly rather than through the factory, with formats that compare by value: a format
 * is part of the source's identity and the built-in ones compare by reference, so two independently
 * discovered instances would make every equality assertion pass or fail for the wrong reason.
 */
class PubSubDynamicSourceTest {

    /** A decoding format that can be given metadata keys of its own, and compares by value. */
    private static final class TestDecodingFormat
            implements DecodingFormat<DeserializationSchema<RowData>> {

        private final String name;
        private final Map<String, DataType> declaredMetadata;
        private final ChangelogMode changelogMode;
        private final List<String> appliedMetadata = new ArrayList<>();

        private TestDecodingFormat(
                String name, Map<String, DataType> declaredMetadata, ChangelogMode changelogMode) {
            this.name = name;
            this.declaredMetadata = declaredMetadata;
            this.changelogMode = changelogMode;
        }

        static TestDecodingFormat plain() {
            return new TestDecodingFormat(
                    "plain", Collections.emptyMap(), ChangelogMode.insertOnly());
        }

        @Override
        public DeserializationSchema<RowData> createRuntimeDecoder(
                DynamicTableSource.Context context, DataType physicalDataType) {
            throw new UnsupportedOperationException("not needed by these tests");
        }

        @Override
        public Map<String, DataType> listReadableMetadata() {
            return declaredMetadata;
        }

        @Override
        public void applyReadableMetadata(List<String> metadataKeys) {
            if (declaredMetadata.isEmpty()) {
                // What every format that declares no metadata does: DecodingFormat's default
                // implementation of this method throws.
                throw new UnsupportedOperationException(
                        "A decoding format must override this method to apply metadata keys.");
            }
            appliedMetadata.clear();
            appliedMetadata.addAll(metadataKeys);
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return changelogMode;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof TestDecodingFormat && name.equals(((TestDecodingFormat) o).name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }
    }

    private static final DataType PHYSICAL_DATA_TYPE =
            DataTypes.ROW(DataTypes.FIELD("id", DataTypes.STRING()));

    private static final DataType PRODUCED_DATA_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.STRING()),
                    DataTypes.FIELD("m", DataTypes.STRING()));

    private static final List<SubscriptionDestination> SUBSCRIPTIONS =
            Collections.singletonList(SubscriptionDestination.of("my-project", "my-sub"));

    private static PubSubDynamicSource source() {
        return source(TestDecodingFormat.plain());
    }

    private static PubSubDynamicSource source(DecodingFormat<DeserializationSchema<RowData>> fmt) {
        return new PubSubDynamicSource(
                PHYSICAL_DATA_TYPE,
                fmt,
                SUBSCRIPTIONS,
                null,
                null,
                PubSubSubscriberOptions.defaults(),
                null,
                null);
    }

    @Test
    void listsTheFormatsMetadataFirstAndItsOwnSecond() {
        Map<String, DataType> formatMetadata = new LinkedHashMap<>();
        formatMetadata.put("ingestion-timestamp", DataTypes.BIGINT());
        PubSubDynamicSource source =
                source(
                        new TestDecodingFormat(
                                "with-metadata", formatMetadata, ChangelogMode.insertOnly()));

        assertThat(source.listReadableMetadata().keySet())
                .containsExactly(
                        "ingestion-timestamp",
                        "message-id",
                        "publish-time",
                        "attributes",
                        "ordering-key",
                        "subscription");
    }

    @Test
    void rejectsAFormatDeclaringOneOfTheConnectorsKeys() {
        Map<String, DataType> colliding =
                Collections.singletonMap("message-id", DataTypes.STRING());
        PubSubDynamicSource source =
                source(new TestDecodingFormat("colliding", colliding, ChangelogMode.insertOnly()));

        assertThatThrownBy(source::listReadableMetadata)
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'message-id'");
    }

    @Test
    void splitsTheSelectedKeysBetweenTheFormatAndItself() {
        Map<String, DataType> formatMetadata =
                Collections.singletonMap("ingestion-timestamp", DataTypes.BIGINT());
        TestDecodingFormat format =
                new TestDecodingFormat("with-metadata", formatMetadata, ChangelogMode.insertOnly());
        PubSubDynamicSource source = source(format);

        source.applyReadableMetadata(
                Arrays.asList("ingestion-timestamp", "message-id"), PRODUCED_DATA_TYPE);

        assertThat(format.appliedMetadata).containsExactly("ingestion-timestamp");
        // The connector's half shows up as a difference in identity, which is all that is exposed.
        assertThat(source).isNotEqualTo(source(format));
    }

    @Test
    void doesNotCallAFormatThatDeclaredNoMetadata() {
        // DecodingFormat.applyReadableMetadata throws by default and no built-in format overrides
        // it, so calling it unconditionally breaks every table with a metadata column — which is
        // exactly what the acceptance IT caught before this guard existed.
        PubSubDynamicSource source = source();

        source.applyReadableMetadata(Collections.singletonList("message-id"), PRODUCED_DATA_TYPE);

        assertThat(source).isNotEqualTo(source());
    }

    @Test
    void takesItsChangelogModeFromTheFormat() {
        ChangelogMode all = ChangelogMode.all();

        assertThat(
                        source(new TestDecodingFormat("changelog", Collections.emptyMap(), all))
                                .getChangelogMode())
                .isEqualTo(all);
        assertThat(source().getChangelogMode()).isEqualTo(ChangelogMode.insertOnly());
    }

    @Test
    void describesItself() {
        assertThat(source().asSummaryString()).isEqualTo("Pub/Sub table source");
    }

    @Test
    void copiesCarryTheAppliedMetadataAndTheProducedType() {
        PubSubDynamicSource original = source();
        original.applyReadableMetadata(Collections.singletonList("message-id"), PRODUCED_DATA_TYPE);

        DynamicTableSource copy = original.copy();

        assertThat(copy).isEqualTo(original).hasSameHashCodeAs(original);
        assertThat(copy).isNotEqualTo(source());
    }

    @Test
    void everyFieldOfTheSourceIsPartOfItsIdentity() {
        PubSubSubscriberOptions defaults = PubSubSubscriberOptions.defaults();

        assertThat(source())
                .isNotEqualTo(
                        new PubSubDynamicSource(
                                DataTypes.ROW(DataTypes.FIELD("other", DataTypes.INT())),
                                TestDecodingFormat.plain(),
                                SUBSCRIPTIONS,
                                null,
                                null,
                                defaults,
                                null,
                                null))
                .isNotEqualTo(
                        source(
                                new TestDecodingFormat(
                                        "other",
                                        Collections.emptyMap(),
                                        ChangelogMode.insertOnly())))
                .isNotEqualTo(
                        new PubSubDynamicSource(
                                PHYSICAL_DATA_TYPE,
                                TestDecodingFormat.plain(),
                                Collections.singletonList(
                                        SubscriptionDestination.of("my-project", "other-sub")),
                                null,
                                null,
                                defaults,
                                null,
                                null))
                .isNotEqualTo(
                        new PubSubDynamicSource(
                                PHYSICAL_DATA_TYPE,
                                TestDecodingFormat.plain(),
                                SUBSCRIPTIONS,
                                OrderingMode.PER_KEY,
                                null,
                                defaults,
                                null,
                                null))
                .isNotEqualTo(
                        new PubSubDynamicSource(
                                PHYSICAL_DATA_TYPE,
                                TestDecodingFormat.plain(),
                                SUBSCRIPTIONS,
                                null,
                                DeserializationFailurePolicy.DROP,
                                defaults,
                                null,
                                null))
                .isNotEqualTo(
                        new PubSubDynamicSource(
                                PHYSICAL_DATA_TYPE,
                                TestDecodingFormat.plain(),
                                SUBSCRIPTIONS,
                                null,
                                null,
                                PubSubSubscriberOptions.builder().parallelPullCount(3).build(),
                                null,
                                null))
                .isNotEqualTo(
                        new PubSubDynamicSource(
                                PHYSICAL_DATA_TYPE,
                                TestDecodingFormat.plain(),
                                SUBSCRIPTIONS,
                                null,
                                null,
                                defaults,
                                "localhost:8085",
                                null))
                .isNotEqualTo(
                        new PubSubDynamicSource(
                                PHYSICAL_DATA_TYPE,
                                TestDecodingFormat.plain(),
                                SUBSCRIPTIONS,
                                null,
                                null,
                                defaults,
                                null,
                                4));
    }

    @Test
    void sourcesBuiltFromTheSameValuesAreEqual() {
        assertThat(source()).isEqualTo(source()).hasSameHashCodeAs(source());
    }

    @Test
    void aSourceIsNotEqualToNullOrToAnotherType() {
        assertThat(source()).isNotEqualTo(null).isNotEqualTo("Pub/Sub table source");
    }

    @Test
    void buildsASourceProviderCarryingTheParallelismAndTheAppliedMetadata() {
        PubSubDynamicSource source =
                new PubSubDynamicSource(
                        PHYSICAL_DATA_TYPE,
                        new DecodingTestFormat(),
                        SUBSCRIPTIONS,
                        OrderingMode.PER_KEY,
                        DeserializationFailurePolicy.DROP,
                        PubSubSubscriberOptions.defaults(),
                        "localhost:8085",
                        4);
        source.applyReadableMetadata(Collections.singletonList("subscription"), PRODUCED_DATA_TYPE);

        ScanTableSource.ScanRuntimeProvider provider =
                source.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        assertThat(provider).isInstanceOf(SourceProvider.class);
        assertThat(((SourceProvider) provider).getParallelism()).contains(4);
        assertThat(((SourceProvider) provider).createSource()).isNotNull();
    }

    @Test
    void leavesTheSourceParallelismUnsetWhenItWasNotGiven() {
        PubSubDynamicSource source =
                new PubSubDynamicSource(
                        PHYSICAL_DATA_TYPE,
                        new DecodingTestFormat(),
                        SUBSCRIPTIONS,
                        null,
                        null,
                        PubSubSubscriberOptions.defaults(),
                        null,
                        null);

        ScanTableSource.ScanRuntimeProvider provider =
                source.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        assertThat(((SourceProvider) provider).getParallelism()).isEmpty();
    }

    @Test
    void listedMetadataIsUnchangedByApplying() {
        PubSubDynamicSource source = source();
        Map<String, DataType> before = source.listReadableMetadata();

        source.applyReadableMetadata(Collections.singletonList("message-id"), PRODUCED_DATA_TYPE);

        assertThat(source.listReadableMetadata()).isEqualTo(before);
    }

    /** A format whose decoder actually exists, so a runtime provider can be built from it. */
    private static final class DecodingTestFormat
            implements DecodingFormat<DeserializationSchema<RowData>> {

        @Override
        public DeserializationSchema<RowData> createRuntimeDecoder(
                DynamicTableSource.Context context, DataType physicalDataType) {
            return new DeserializationSchema<RowData>() {

                private static final long serialVersionUID = 1L;

                @Override
                public RowData deserialize(byte[] message) {
                    return null;
                }

                @Override
                public boolean isEndOfStream(RowData nextElement) {
                    return false;
                }

                @Override
                public TypeInformation<RowData> getProducedType() {
                    return InternalTypeInfo.of(RowType.of(new VarCharType(VarCharType.MAX_LENGTH)));
                }
            };
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return ChangelogMode.insertOnly();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof DecodingTestFormat;
        }

        @Override
        public int hashCode() {
            return DecodingTestFormat.class.hashCode();
        }
    }
}
