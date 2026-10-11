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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.datastore.BaseEntity;
import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.EntityValue;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.LatLng;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.Value;
import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Mutation;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.PropertyMask;
import com.google.protobuf.NullValue;
import com.google.protobuf.UnsafeByteOperations;
import io.github.flink.gcp.connector.datastore.PropertyPaths;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;

import java.util.ArrayList;
import java.util.List;

/**
 * Encodes a {@link DatastoreMutation} as the {@code google.datastore.v1.Mutation} the writer
 * commits, since the client library's {@code Batch} cannot carry the property mask an {@link
 * DatastoreMutation.Operation#UPSERT_MERGE} needs.
 *
 * <p>The library's own conversion is package-private ({@code BaseEntity.toPb}, {@code Value.toPb},
 * {@code Key.toPb}; google-cloud-datastore 3.7.0), so this repeats it value type by value type:
 * each value carries its {@code exclude_from_indexes} mark and its meaning, read through {@code
 * toBuilder()}, the only public way to it. {@code EntityProtosTest} holds the result equal to what
 * the library makes of the same entity — {@code FullEntity.fromPb} reads it back equal, and its
 * size is the library's {@code calculateSerializedSize} — for every value type.
 */
@Internal
final class EntityProtos {

    private EntityProtos() {}

    /**
     * Returns the mutation as the {@code Commit} request carries it.
     *
     * @param write the write
     * @return its protobuf form
     */
    static Mutation mutation(DatastoreMutation write) {
        Mutation.Builder mutation = Mutation.newBuilder();
        switch (write.getOperation()) {
            case UPSERT:
                return mutation.setUpsert(entity(entityOf(write))).build();
            case UPSERT_MERGE:
                FullEntity<?> merged = entityOf(write);
                return mutation.setUpsert(entity(merged))
                        .setPropertyMask(PropertyMask.newBuilder().addAllPaths(mergePaths(merged)))
                        .build();
            case INSERT:
                return mutation.setInsert(entity(entityOf(write))).build();
            case UPDATE:
                return mutation.setUpdate(entity(entityOf(write))).build();
            case DELETE:
                return mutation.setDelete(key(write.getKey())).build();
            default:
                throw new IllegalStateException(
                        "Unknown Datastore operation " + write.getOperation());
        }
    }

    private static FullEntity<?> entityOf(DatastoreMutation write) {
        return Preconditions.checkNotNull(
                write.getEntity(), "A %s carries an entity", write.getOperation());
    }

    /**
     * Returns the property mask paths a merge of the entity writes: one per value it holds, an
     * embedded entity contributing its own values' paths rather than itself, so that one without
     * properties contributes none and leaves the stored property as it is, as the Native-mode
     * client library's merge treats an empty map ({@code FieldMask.fromObject}). Each property name
     * is one {@link PropertyPaths#segment(String)}, so a name holding a dot stays one property;
     * segments are joined by dots. A path stops at an array, which the mask cannot enter.
     *
     * @param entity the entity a merge writes
     * @return the paths, in the entity's property order
     */
    static List<String> mergePaths(BaseEntity<?> entity) {
        List<String> paths = new ArrayList<>();
        addMergePaths(entity, "", paths);
        return paths;
    }

    private static void addMergePaths(BaseEntity<?> entity, String prefix, List<String> paths) {
        for (String name : entity.getNames()) {
            String path = prefix + PropertyPaths.segment(name);
            Value<?> value = entity.getValue(name);
            if (value instanceof EntityValue) {
                addMergePaths(((EntityValue) value).get(), path + ".", paths);
            } else {
                paths.add(path);
            }
        }
    }

    /**
     * Returns the entity's protobuf form, its key included when it has one.
     *
     * @param entity the entity, a top-level one or one embedded in a value
     * @return its protobuf form
     */
    static Entity entity(BaseEntity<?> entity) {
        Entity.Builder proto = Entity.newBuilder();
        IncompleteKey key = entity.getKey();
        if (key != null) {
            proto.setKey(key(key));
        }
        for (String name : entity.getNames()) {
            proto.putProperties(name, value(entity.getValue(name)));
        }
        return proto.build();
    }

    /**
     * Returns the value's protobuf form, with its index mark and meaning.
     *
     * @param value the value
     * @return its protobuf form
     */
    @SuppressWarnings("deprecation")
    private static com.google.datastore.v1.Value value(Value<?> value) {
        com.google.datastore.v1.Value.Builder proto =
                com.google.datastore.v1.Value.newBuilder()
                        .setExcludeFromIndexes(value.excludeFromIndexes())
                        .setMeaning(value.toBuilder().getMeaning());
        Object content = value.get();
        switch (value.getType()) {
            case NULL:
                return proto.setNullValue(NullValue.NULL_VALUE).build();
            case STRING:
                return proto.setStringValue((String) content).build();
            case LONG:
                return proto.setIntegerValue((Long) content).build();
            case DOUBLE:
                return proto.setDoubleValue((Double) content).build();
            case BOOLEAN:
                return proto.setBooleanValue((Boolean) content).build();
            case TIMESTAMP:
                return proto.setTimestampValue(((com.google.cloud.Timestamp) content).toProto())
                        .build();
            case LAT_LNG:
                LatLng point = (LatLng) content;
                return proto.setGeoPointValue(
                                com.google.type.LatLng.newBuilder()
                                        .setLatitude(point.getLatitude())
                                        .setLongitude(point.getLongitude()))
                        .build();
            case BLOB:
                // Wrapped, not copied: a Blob is immutable, and the writer keeps the encoding
                // beside the entity until the commit, so a copy would hold the bytes twice.
                return proto.setBlobValue(
                                UnsafeByteOperations.unsafeWrap(
                                        ((Blob) content).asReadOnlyByteBuffer()))
                        .build();
            case KEY:
                return proto.setKeyValue(key((IncompleteKey) content)).build();
            case ENTITY:
                return proto.setEntityValue(entity((FullEntity<?>) content)).build();
            case LIST:
                ArrayValue.Builder array = ArrayValue.newBuilder();
                for (Object element : (List<?>) content) {
                    array.addValues(value((Value<?>) element));
                }
                return proto.setArrayValue(array).build();
            case RAW_VALUE:
                // A value the library holds in its protobuf form: merged over the mark and meaning,
                // as the library does.
                return proto.mergeFrom((com.google.datastore.v1.Value) content).build();
            default:
                throw new IllegalArgumentException(
                        "A Datastore value of type " + value.getType() + " cannot be written.");
        }
    }

    /**
     * Returns the key's protobuf form: its partition and its path, the last element without an id
     * or a name when the key is incomplete.
     *
     * @param key the key
     * @return its protobuf form
     */
    static Key key(IncompleteKey key) {
        Key.Builder proto =
                Key.newBuilder()
                        .setPartitionId(
                                PartitionId.newBuilder()
                                        .setProjectId(key.getProjectId())
                                        .setDatabaseId(key.getDatabaseId())
                                        .setNamespaceId(key.getNamespace()));
        for (PathElement ancestor : key.getAncestors()) {
            proto.addPath(
                    element(
                            ancestor.getKind(),
                            ancestor.hasId() ? ancestor.getId() : null,
                            ancestor.getName()));
        }
        if (key instanceof com.google.cloud.datastore.Key) {
            com.google.cloud.datastore.Key complete = (com.google.cloud.datastore.Key) key;
            proto.addPath(element(complete.getKind(), complete.getId(), complete.getName()));
        } else {
            proto.addPath(element(key.getKind(), null, null));
        }
        return proto.build();
    }

    private static Key.PathElement element(String kind, Long id, String name) {
        Key.PathElement.Builder element = Key.PathElement.newBuilder().setKind(kind);
        if (id != null) {
            element.setId(id);
        } else if (name != null) {
            element.setName(name);
        }
        return element.build();
    }
}
