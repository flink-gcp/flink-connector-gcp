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

package io.github.flink.gcp.connector.firestore.sink.writer;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Blob;
import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.GeoPoint;
import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.BatchWriteRequest;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.DocumentMask;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Precondition;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Write;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.google.type.LatLng;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.sink.FirestoreDocumentReference;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Holds the estimate equal to the serialized size of the {@code Write} the client library builds,
 * reconstructed here from the proto classes the way the library builds it: literal field names, an
 * update mask of top-level fields for an update and of every leaf for a merge, and the precondition
 * each operation carries. The size is taken inside a {@code BatchWriteRequest}, which adds the
 * field tag and length the estimate counts.
 *
 * <p>The reconstruction is this test's reading of the library, so a misreading shared with the
 * estimator would pass here; {@code DocumentSizeEstimatorITCase} compares the same writes with the
 * requests the library sends.
 */
class DocumentSizeEstimatorTest {

    private static final DatabaseDestination DATABASE = DatabaseDestination.of("p", "db");
    private static final String ROOT = "projects/p/databases/db/documents/";

    private final DocumentSizeEstimator estimator = new DocumentSizeEstimator(DATABASE);

    @Test
    void everyValueTypeIsCountedAsTheWireEncodesIt() {
        assertMatches(FirestoreWrite.set("c/a", everyValueType()));
    }

    @Test
    void manySmallFieldsAreCountedAtTheirWireSize() {
        assertMatches(FirestoreWrite.set("c/a", manySmallFields()));
    }

    @Test
    void everyOperationIsCountedWithItsMaskAndPrecondition() {
        for (FirestoreWrite write : everyOperation()) {
            assertMatches(write);
        }
    }

    /**
     * The writes these tests size, which {@code DocumentSizeEstimatorITCase} also sends through the
     * client library to compare the estimate with the request the library actually builds.
     */
    static List<FirestoreWrite> writesOfEveryShape() {
        List<FirestoreWrite> writes = new ArrayList<>();
        writes.add(FirestoreWrite.set("c/a", everyValueType()));
        writes.add(FirestoreWrite.set("c/a", manySmallFields()));
        writes.addAll(everyOperation());
        return writes;
    }

    private static Map<String, Object> everyValueType() {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("null", null);
        fields.put("true", true);
        fields.put("false", false);
        fields.put("zero", 0L);
        fields.put("negative", -1L);
        fields.put("large", Long.MAX_VALUE);
        fields.put("double", 1.5d);
        fields.put("empty", "");
        fields.put("unicode", "éあ😀");
        fields.put("epoch", Timestamp.ofTimeSecondsAndNanos(0, 0));
        fields.put("time", Timestamp.ofTimeSecondsAndNanos(1_700_000_000, 123));
        fields.put("origin", new GeoPoint(0, 0));
        fields.put("point", new GeoPoint(35.6, 139.7));
        // -0.0 == 0 in Java, but protobuf compares raw bits and sends a negative zero.
        fields.put("negativeZero", new GeoPoint(-0.0, -0.0));
        fields.put("mixedZero", new GeoPoint(0.0, -0.0));
        fields.put("negativeZeroDouble", -0.0d);
        // An unpaired surrogate is one replacement byte on the wire, not three.
        fields.put("unpairedHigh", "a\uD800b");
        fields.put("unpairedLow\uDC00", "\uDC00");
        fields.put("blob", Blob.fromBytes(new byte[300]));
        fields.put("list", List.of(1L, List.of("nested"), Map.of("k", false)));
        fields.put("map", Map.of("inner", Map.of("deeper", "v"), "emptyMap", Map.of()));
        fields.put("reference", FirestoreDocumentReference.of("users/alice"));
        fields.put("unicodeReference", FirestoreDocumentReference.of("c/éあ😀/sub/x"));
        fields.put(
                "nestedReferences",
                Map.of(
                        "inMap",
                        FirestoreDocumentReference.of("c/a"),
                        "inList",
                        List.of(FirestoreDocumentReference.of("c/b"), 1L)));
        fields.put(
                "listedReferences",
                List.of(
                        Map.of("inMapInList", FirestoreDocumentReference.of("c/d")),
                        FirestoreDocumentReference.of("c/e")));
        return fields;
    }

    private static Map<String, Object> manySmallFields() {
        // The shape where the storage-size formula undercounts the wire by about half.
        Map<String, Object> fields = new LinkedHashMap<>();
        for (int i = 0; i < 10_000; i++) {
            fields.put("f" + i, true);
        }
        return fields;
    }

    private static List<FirestoreWrite> everyOperation() {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("plain", 1L);
        fields.put("a.b", "a name that is not an identifier");
        fields.put("back`tick\\slash", "escaped");
        fields.put("9lives", "a leading digit");
        fields.put("nested", Map.of("leaf", 1L, "deeper", Map.of("x", 2L), "none", Map.of()));
        fields.put("ref", Map.of("leafReference", FirestoreDocumentReference.of("c/b")));
        Timestamp time = Timestamp.ofTimeSecondsAndNanos(1_700_000_000, 5);

        return List.of(
                FirestoreWrite.set("c/a", fields),
                FirestoreWrite.setMerge("c/a", fields),
                FirestoreWrite.create("c/a/sub/b", fields),
                FirestoreWrite.update("c/a", fields),
                FirestoreWrite.update("c/a", fields, time),
                FirestoreWrite.delete("c/a"),
                FirestoreWrite.delete("c/a", time));
    }

    private void assertMatches(FirestoreWrite write) {
        BatchWriteRequest request = BatchWriteRequest.newBuilder().addWrites(proto(write)).build();

        assertThat(estimator.estimate(write))
                .as("%s", write)
                .isEqualTo(request.getSerializedSize());
    }

    private static Write proto(FirestoreWrite write) {
        String name = ROOT + write.getDocumentPath();
        Write.Builder builder = Write.newBuilder();
        switch (write.getOperation()) {
            case DELETE:
                builder.setDelete(name);
                break;
            case SET_MERGE:
                builder.setUpdateMask(
                        DocumentMask.newBuilder()
                                .addAllFieldPaths(leafPaths(write.getFields(), new ArrayList<>())));
                builder.setUpdate(document(name, write.getFields()));
                break;
            case UPDATE:
                DocumentMask.Builder mask = DocumentMask.newBuilder();
                for (String key : write.getFields().keySet()) {
                    mask.addFieldPaths(encoded(List.of(key)));
                }
                builder.setUpdateMask(mask);
                builder.setUpdate(document(name, write.getFields()));
                break;
            default:
                builder.setUpdate(document(name, write.getFields()));
        }
        if (write.getLastUpdateTime() != null) {
            builder.setCurrentDocument(
                    Precondition.newBuilder().setUpdateTime(write.getLastUpdateTime().toProto()));
        } else if (write.getOperation() == FirestoreWrite.Operation.CREATE) {
            builder.setCurrentDocument(Precondition.newBuilder().setExists(false));
        } else if (write.getOperation() == FirestoreWrite.Operation.UPDATE) {
            builder.setCurrentDocument(Precondition.newBuilder().setExists(true));
        }
        return builder.build();
    }

    private static Document document(String name, Map<String, Object> fields) {
        Document.Builder document = Document.newBuilder().setName(name);
        fields.forEach((key, value) -> document.putFields(key, value(value)));
        return document.build();
    }

    @SuppressWarnings("unchecked")
    private static Value value(Object value) {
        Value.Builder builder = Value.newBuilder();
        if (value == null) {
            builder.setNullValue(NullValue.NULL_VALUE);
        } else if (value instanceof Boolean) {
            builder.setBooleanValue((Boolean) value);
        } else if (value instanceof Long) {
            builder.setIntegerValue((Long) value);
        } else if (value instanceof Double) {
            builder.setDoubleValue((Double) value);
        } else if (value instanceof String) {
            builder.setStringValue((String) value);
        } else if (value instanceof Timestamp) {
            builder.setTimestampValue(((Timestamp) value).toProto());
        } else if (value instanceof GeoPoint) {
            GeoPoint point = (GeoPoint) value;
            builder.setGeoPointValue(
                    LatLng.newBuilder()
                            .setLatitude(point.getLatitude())
                            .setLongitude(point.getLongitude()));
        } else if (value instanceof FirestoreDocumentReference) {
            builder.setReferenceValue(
                    ROOT + ((FirestoreDocumentReference) value).getDocumentPath());
        } else if (value instanceof Blob) {
            builder.setBytesValue(ByteString.copyFrom(((Blob) value).toBytes()));
        } else if (value instanceof List) {
            ArrayValue.Builder array = ArrayValue.newBuilder();
            for (Object element : (List<Object>) value) {
                array.addValues(value(element));
            }
            builder.setArrayValue(array);
        } else {
            MapValue.Builder map = MapValue.newBuilder();
            ((Map<String, Object>) value).forEach((k, v) -> map.putFields(k, value(v)));
            builder.setMapValue(map);
        }
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private static List<String> leafPaths(Map<String, Object> fields, List<String> prefix) {
        List<String> paths = new ArrayList<>();
        fields.forEach(
                (key, value) -> {
                    List<String> path = new ArrayList<>(prefix);
                    path.add(key);
                    if (value instanceof Map) {
                        paths.addAll(leafPaths((Map<String, Object>) value, path));
                    } else {
                        paths.add(encoded(path));
                    }
                });
        return paths;
    }

    /** The library's own encoding of a field path, read through its public string form. */
    private static String encoded(List<String> segments) {
        return FieldPath.of(segments.toArray(new String[0])).toString();
    }
}
