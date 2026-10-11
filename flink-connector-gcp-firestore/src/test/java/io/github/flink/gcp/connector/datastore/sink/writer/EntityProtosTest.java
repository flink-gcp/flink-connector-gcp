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

package io.github.flink.gcp.connector.datastore.sink.writer;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.BlobValue;
import com.google.cloud.datastore.BooleanValue;
import com.google.cloud.datastore.DoubleValue;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityValue;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyValue;
import com.google.cloud.datastore.LatLng;
import com.google.cloud.datastore.LatLngValue;
import com.google.cloud.datastore.ListValue;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.NullValue;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.TimestampValue;
import com.google.cloud.datastore.Value;
import com.google.cloud.datastore.ValueType;
import com.google.datastore.v1.Mutation;
import com.google.datastore.v1.PropertyMask;
import com.google.protobuf.TextFormat;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;
import org.junit.jupiter.api.Test;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Holds {@link EntityProtos} equal to the client library's own conversion, which is
 * package-private: an entity encoded here reads back through the library's public {@code fromPb} as
 * the same entity, at the size the library's {@code calculateSerializedSize} reports, for every
 * value type, index mark and meaning.
 */
class EntityProtosTest {

    private static final Key KEY =
            Key.newBuilder("p", "Order", "o-1", "orders-db").setNamespace("tenant").build();

    /**
     * The lookup and a delete encode a key here; the client library's own encoding is reachable
     * only as {@code Key.toUrlSafe()}, a URL-encoded protobuf text form, which this decodes to
     * compare.
     */
    @Test
    void aKeyIsEncodedAsTheClientLibraryEncodesIt() throws Exception {
        Key[] keys = {
            Key.newBuilder("p", "Order", "o-1").build(),
            Key.newBuilder("p", "Order", 7L).build(),
            Key.newBuilder("p", "Order", "o-1", "orders-db")
                    .setNamespace("tenant")
                    .addAncestor(PathElement.of("Customer", 3L))
                    .addAncestor(PathElement.of("Account", "a-1"))
                    .build()
        };
        for (Key key : keys) {
            com.google.datastore.v1.Key.Builder expected = com.google.datastore.v1.Key.newBuilder();
            TextFormat.merge(URLDecoder.decode(key.toUrlSafe(), StandardCharsets.UTF_8), expected);

            assertThat(EntityProtos.key(key)).as(key.toString()).isEqualTo(expected.build());
        }
    }

    @Test
    void everyValueTypeReadsBackAsTheSameEntityAtTheLibrarysSize() throws Exception {
        Entity entity = everyValueType();
        Set<ValueType> covered = EnumSet.noneOf(ValueType.class);
        collectTypes(entity, covered);
        // A value type a libraries-bom bump adds fails here until the conversion names it.
        assertThat(covered).containsExactlyInAnyOrder(ValueType.values());

        com.google.datastore.v1.Entity proto = EntityProtos.entity(entity);

        assertThat(Entity.fromPb(proto)).isEqualTo(entity);
        assertThat(proto.getSerializedSize()).isEqualTo(Entity.calculateSerializedSize(entity));
        // Through the wire form too, which a value the encoding wraps rather than copies (a blob)
        // takes a path of its own to.
        assertThat(Entity.fromPb(com.google.datastore.v1.Entity.parseFrom(proto.toByteArray())))
                .isEqualTo(entity);
    }

    @Test
    void theIndexMarkAndTheMeaningAreKept() {
        @SuppressWarnings("deprecation")
        Entity entity =
                Entity.newBuilder(KEY)
                        .set(
                                "excluded",
                                StringValue.newBuilder("x").setExcludeFromIndexes(true).build())
                        .set("meant", LongValue.newBuilder(5L).setMeaning(18).build())
                        .build();

        com.google.datastore.v1.Entity proto = EntityProtos.entity(entity);

        assertThat(proto.getPropertiesOrThrow("excluded").getExcludeFromIndexes()).isTrue();
        assertThat(proto.getPropertiesOrThrow("meant").getMeaning()).isEqualTo(18);
        assertThat(Entity.fromPb(proto)).isEqualTo(entity);
        assertThat(proto.getSerializedSize()).isEqualTo(Entity.calculateSerializedSize(entity));
    }

    @Test
    void eachOperationIsItsMutationAndOnlyAMergeCarriesAMask() {
        Entity entity = Entity.newBuilder(KEY).set("v", 1L).build();
        com.google.datastore.v1.Entity proto = EntityProtos.entity(entity);

        assertThat(EntityProtos.mutation(DatastoreMutation.upsert(entity)))
                .isEqualTo(Mutation.newBuilder().setUpsert(proto).build());
        assertThat(EntityProtos.mutation(DatastoreMutation.insert(entity)))
                .isEqualTo(Mutation.newBuilder().setInsert(proto).build());
        assertThat(EntityProtos.mutation(DatastoreMutation.update(entity)))
                .isEqualTo(Mutation.newBuilder().setUpdate(proto).build());
        assertThat(EntityProtos.mutation(DatastoreMutation.delete(KEY)))
                .isEqualTo(Mutation.newBuilder().setDelete(EntityProtos.key(KEY)).build());
        assertThat(EntityProtos.mutation(DatastoreMutation.upsertMerge(entity)))
                .isEqualTo(
                        Mutation.newBuilder()
                                .setUpsert(proto)
                                .setPropertyMask(PropertyMask.newBuilder().addPaths("`v`"))
                                .build());
    }

    @Test
    void aMergeNamesEveryValueStopsAtAnArrayAndANullAndSkipsAnEmptyEntity() {
        FullEntity<IncompleteKey> inner =
                FullEntity.newBuilder().set("x", 1L).set("deeper", entityOf("z", 2L)).build();
        Entity entity =
                Entity.newBuilder(KEY)
                        .set("top", "t")
                        .set("e", inner)
                        .set("empty", FullEntity.newBuilder().build())
                        .set("list", ListValue.of(EntityValue.of(entityOf("q", 1L))))
                        .set("gone", NullValue.of())
                        .build();

        assertThat(EntityProtos.mergePaths(entity))
                .containsExactlyInAnyOrder(
                        "`top`", "`e`.`x`", "`e`.`deeper`.`z`", "`list`", "`gone`");
    }

    @Test
    void aMergePathQuotesEachNameSoADotStaysInsideIt() {
        Entity entity =
                Entity.newBuilder(KEY)
                        .set("a.b", 1L)
                        .set("tick`", 1L)
                        .set("back\\slash", 1L)
                        .set("e.f", entityOf("g.h", 1L))
                        .build();

        assertThat(EntityProtos.mergePaths(entity))
                .containsExactlyInAnyOrder("`a.b`", "`tick\\``", "`back\\\\slash`", "`e.f`.`g.h`");
    }

    @Test
    void aMergeOfAnEntityWithoutPropertiesHasAnEmptyMask() {
        Entity keyOnly = Entity.newBuilder(KEY).build();

        Mutation mutation = EntityProtos.mutation(DatastoreMutation.upsertMerge(keyOnly));

        assertThat(mutation.hasPropertyMask()).isTrue();
        assertThat(mutation.getPropertyMask().getPathsList()).isEmpty();
    }

    private static Entity everyValueType() {
        FullEntity<IncompleteKey> keyless = entityOf("inner", 1L);
        FullEntity<IncompleteKey> incomplete =
                FullEntity.newBuilder(IncompleteKey.newBuilder("p", "Child").build())
                        .set("inner", "x")
                        .build();
        return Entity.newBuilder(KEY)
                .set("null", NullValue.of())
                .set("string", StringValue.of("s"))
                .set("long", LongValue.of(-7L))
                .set("double", DoubleValue.of(1.5))
                .set("boolean", BooleanValue.of(true))
                .set("timestamp", TimestampValue.of(Timestamp.ofTimeMicroseconds(1_234_567L)))
                .set("point", LatLngValue.of(LatLng.of(35.6, 139.7)))
                .set("blob", BlobValue.of(Blob.copyFrom(new byte[] {0, 1, 2})))
                .set("key", KeyValue.of(Key.newBuilder("p", "Other", 3L).build()))
                .set("entity", EntityValue.of(keyless))
                .set("childEntity", EntityValue.of(incomplete))
                .set(
                        "list",
                        ListValue.of(
                                StringValue.of("a"),
                                LongValue.of(1L),
                                EntityValue.of(keyless),
                                NullValue.of()))
                .set(
                        "raw",
                        // A value of no type the library knows, which it keeps in protobuf form.
                        Value.fromPb(
                                com.google.datastore.v1.Value.newBuilder()
                                        .setExcludeFromIndexes(true)
                                        .build()))
                .build();
    }

    private static FullEntity<IncompleteKey> entityOf(String name, long value) {
        return FullEntity.newBuilder().set(name, value).build();
    }

    private static void collectTypes(FullEntity<?> entity, Set<ValueType> types) {
        for (String name : entity.getNames()) {
            collectTypes(entity.getValue(name), types);
        }
    }

    private static void collectTypes(Value<?> value, Set<ValueType> types) {
        types.add(value.getType());
        if (value instanceof EntityValue) {
            collectTypes(((EntityValue) value).get(), types);
        } else if (value instanceof ListValue) {
            for (Value<?> element : ((ListValue) value).get()) {
                collectTypes(element, types);
            }
        }
    }
}
