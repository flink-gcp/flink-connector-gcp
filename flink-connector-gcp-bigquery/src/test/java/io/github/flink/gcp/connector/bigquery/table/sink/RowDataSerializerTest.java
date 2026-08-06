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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RowDataSerializer}. */
class RowDataSerializerTest {

    private static final TableDestination DESTINATION =
            TableDestination.of("my-project", "my_dataset", "my_table");

    private static RowType rowType() {
        return (RowType)
                DataTypes.ROW(
                                DataTypes.FIELD("name", DataTypes.STRING()),
                                DataTypes.FIELD("amount", DataTypes.BIGINT()))
                        .getLogicalType();
    }

    @Test
    void derivesTheSchemaAndTheDescriptorFromTheRowType() {
        RowDataSerializer serializer =
                new RowDataSerializer(rowType(), RowDataSchemaOptions.defaults());

        assertThat(serializer.getTableSchema(DESTINATION).getFieldsList())
                .extracting(TableFieldSchema::getName)
                .containsExactly("name", "amount");
        assertThat(serializer.getDescriptor(DESTINATION).getFields()).hasSize(2);
    }

    @Test
    void theDescriptorIsCachedRatherThanRederivedPerCall() {
        RowDataSerializer serializer =
                new RowDataSerializer(rowType(), RowDataSchemaOptions.defaults());
        assertThat(serializer.getDescriptor(DESTINATION))
                .isSameAs(serializer.getDescriptor(DESTINATION));
    }

    @Test
    void oneSchemaServesEveryDestination() {
        // Fixed-destination only from SQL, but the SPI is per destination — so this pins that the
        // argument is deliberately ignored rather than accidentally unused.
        RowDataSerializer serializer =
                new RowDataSerializer(rowType(), RowDataSchemaOptions.defaults());
        assertThat(serializer.getTableSchema(TableDestination.of("p", "d", "other")))
                .isEqualTo(serializer.getTableSchema(DESTINATION));
    }

    @Test
    void aSchemaProblemFailsInTheConstructorRatherThanFromSerialize() {
        // The eager-derivation rule: from serialize() the failure would run inside the writers'
        // failure handler, where a dropping policy swallows it once per record for the life of the
        // job and leaves the table empty under a green job.
        RowType unmappable =
                (RowType)
                        DataTypes.ROW(DataTypes.FIELD("v", DataTypes.INTERVAL(DataTypes.DAY())))
                                .getLogicalType();
        assertThatThrownBy(() -> new RowDataSerializer(unmappable, RowDataSchemaOptions.defaults()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no BigQuery equivalent");
    }

    @Test
    void theSchemaIsStaticSoThereIsNoFingerprint() {
        // A null fingerprint is what tells the writers this serializer's schema never evolves.
        assertThat(
                        new RowDataSerializer(rowType(), RowDataSchemaOptions.defaults())
                                .getSchemaFingerprint(DESTINATION))
                .isNull();
    }

    @Test
    void serializesARowIntoTheDerivedDescriptorsForm() throws Exception {
        RowDataSerializer serializer =
                new RowDataSerializer(rowType(), RowDataSchemaOptions.defaults());

        DynamicMessage message =
                DynamicMessage.parseFrom(
                        serializer.getDescriptor(DESTINATION),
                        serializer.serialize(
                                GenericRowData.of(StringData.fromString("alice"), 3L)));

        assertThat(message.getField(serializer.getDescriptor(DESTINATION).getFields().get(0)))
                .isEqualTo("alice");
        assertThat(message.getField(serializer.getDescriptor(DESTINATION).getFields().get(1)))
                .isEqualTo(3L);
    }

    @Test
    void survivesTheTripToATaskManager() throws Exception {
        RowDataSerializer serializer =
                new RowDataSerializer(rowType(), RowDataSchemaOptions.defaults());

        // The derived triple is transient, so this rebuilds it on the far side — which is the
        // whole reason the row type and the options are what the class holds.
        RowDataSerializer copy = InstantiationUtil.clone(serializer);

        assertThat(copy.getTableSchema(DESTINATION))
                .isEqualTo(serializer.getTableSchema(DESTINATION));
        assertThat(copy.serialize(GenericRowData.of(StringData.fromString("alice"), 3L)))
                .isEqualTo(
                        serializer.serialize(
                                GenericRowData.of(StringData.fromString("alice"), 3L)));
    }
}
