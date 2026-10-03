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

import org.apache.flink.annotation.Internal;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Blob;
import com.google.cloud.firestore.GeoPoint;
import com.google.protobuf.CodedOutputStream;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.sink.FirestoreDocumentReference;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;

import java.util.List;
import java.util.Map;

/**
 * Computes how many bytes a write adds to a {@code BatchWrite} request, for the writer's in-flight
 * byte bound, its request-size budget and {@code numBytesSend}.
 *
 * <p>The figure is the protobuf wire size of the {@code google.firestore.v1.Write} the client
 * library builds for the write, with its field tag and length inside the request: the document's
 * resource name and fields, the update mask a merge or an update sends, and the precondition a
 * create, an update or a conditional write carries. It is computed from the values rather than by
 * building the message, because the library's conversion to the wire form is package-private
 * ({@code UserDataConverter}, google-cloud-firestore 3.49.0) and building a second copy would cost
 * a second full conversion per record. Firestore's storage-size formula, the unit of its 1 MiB
 * document limit, is not used: for a document of many small fields the wire form is about twice
 * that size, so a request budget kept in storage bytes could pass the request limit.
 *
 * <p>{@code DocumentSizeEstimatorTest} holds the computation equal to the serialized size of the
 * same message built from the proto classes, for every value type {@code FirestoreWrite} admits and
 * every operation.
 */
@Internal
final class DocumentSizeEstimator {

    /**
     * The tag of Value.string_value (17) and Value.bytes_value (18); every other field is below 16.
     */
    private static final int TWO_BYTE_TAG = 2;

    /** The UTF-8 length of the database's documents root, which every resource name starts with. */
    private final long documentsRootLength;

    /**
     * Creates the estimator for writes into one database.
     *
     * @param database the database the writes are bound for
     */
    DocumentSizeEstimator(DatabaseDestination database) {
        this.documentsRootLength = utf8Length(database + "/documents/");
    }

    /**
     * Returns the bytes a write adds to a request.
     *
     * @param write the write
     * @return the size, positive
     */
    int estimate(FirestoreWrite write) {
        long name = resourceName(write.getDocumentPath());
        long body;
        if (write.getOperation() == FirestoreWrite.Operation.DELETE) {
            // Write.delete (2) is the resource name alone.
            body = name;
        } else {
            long fields = 0;
            for (Map.Entry<String, Object> entry : write.getFields().entrySet()) {
                fields += mapEntry(entry.getKey(), entry.getValue());
            }
            // Write.update (1) holds the Document: name (1) and its fields map (2).
            body = delimited(name + fields);
            if (write.getOperation() == FirestoreWrite.Operation.SET_MERGE) {
                body += delimited(mergeMask(write.getFields(), 0));
            } else if (write.getOperation() == FirestoreWrite.Operation.UPDATE) {
                long mask = 0;
                for (String key : write.getFields().keySet()) {
                    mask += delimited(fieldPathLength(key));
                }
                body += delimited(mask);
            }
        }
        body += precondition(write);
        // BatchWriteRequest.writes (2): the Write with its tag and length.
        return (int) Math.min(Integer.MAX_VALUE, delimited(body));
    }

    /**
     * A document's full resource name as a length-delimited field: the write's own name, or a
     * reference value naming another document of the same database.
     */
    private long resourceName(String documentPath) {
        return delimited(documentsRootLength + utf8Length(documentPath));
    }

    /** Write.current_document (4): what the library sends for the operation. */
    private static long precondition(FirestoreWrite write) {
        Timestamp lastUpdateTime = write.getLastUpdateTime();
        if (lastUpdateTime != null) {
            // Precondition.update_time (2).
            return delimited(delimited(timestamp(lastUpdateTime)));
        }
        switch (write.getOperation()) {
            case CREATE:
            case UPDATE:
                // Precondition.exists (1): false for a create, true for an update. A oneof member
                // is sent even when false.
                return delimited(2);
            default:
                return 0;
        }
    }

    /**
     * The mask a merge sends: DocumentMask.field_paths (1), one path per leaf of the fields map, a
     * nested map contributing its leaves rather than itself.
     */
    private static long mergeMask(Map<String, Object> fields, long prefixLength) {
        long mask = 0;
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            long path = prefixLength + fieldPathLength(entry.getKey());
            Object value = entry.getValue();
            if (value instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> nested = (Map<String, Object>) value;
                // Plus one for the dot joining this segment to the next.
                mask += mergeMask(nested, path + 1);
            } else {
                mask += delimited(path);
            }
        }
        return mask;
    }

    /** One map entry, in Document.fields or MapValue.fields: key (1) and value (2). */
    private long mapEntry(String key, Object value) {
        return delimited(delimited(utf8Length(key)) + delimited(value(value)));
    }

    /** The encoded size of one google.firestore.v1.Value, without its own tag and length. */
    private long value(Object value) {
        if (value == null || value instanceof Boolean) {
            // null_value (11), an enum, or boolean_value (1).
            return 2;
        }
        if (value instanceof Long) {
            // integer_value (2).
            return 1 + CodedOutputStream.computeInt64SizeNoTag((Long) value);
        }
        if (value instanceof Double) {
            // double_value (3).
            return 9;
        }
        if (value instanceof String) {
            // string_value (17).
            long length = utf8Length((String) value);
            return TWO_BYTE_TAG + varintSize(length) + length;
        }
        if (value instanceof Timestamp) {
            // timestamp_value (10).
            return delimited(timestamp((Timestamp) value));
        }
        if (value instanceof GeoPoint) {
            // geo_point_value (8): a LatLng of two doubles, each omitted when its bits are zero.
            GeoPoint point = (GeoPoint) value;
            return delimited(coordinate(point.getLatitude()) + coordinate(point.getLongitude()));
        }
        if (value instanceof FirestoreDocumentReference) {
            // reference_value (5): the referenced document's full resource name.
            return resourceName(((FirestoreDocumentReference) value).getDocumentPath());
        }
        if (value instanceof Blob) {
            // bytes_value (18). FirestoreWrite admits only a subtype-0 Blob; the library sends any
            // other subtype as a map_value, which this does not count (#1589).
            long length = ((Blob) value).toByteString().size();
            return TWO_BYTE_TAG + varintSize(length) + length;
        }
        if (value instanceof List) {
            // array_value (9): ArrayValue.values (1), one Value each.
            long elements = 0;
            for (Object element : (List<?>) value) {
                elements += delimited(value(element));
            }
            return delimited(elements);
        }
        // map_value (6): MapValue.fields (1). FirestoreWrite admits nothing else.
        long entries = 0;
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            entries += mapEntry((String) entry.getKey(), entry.getValue());
        }
        return delimited(entries);
    }

    /**
     * One LatLng double: omitted when its raw bits are zero, as protobuf decides, so a negative
     * zero — equal to zero in Java — is sent and counted.
     */
    private static long coordinate(double value) {
        return Double.doubleToRawLongBits(value) == 0 ? 0 : 9;
    }

    /** A google.protobuf.Timestamp: seconds (1) and nanos (2), each omitted when zero. */
    private static long timestamp(Timestamp timestamp) {
        long seconds = timestamp.getSeconds();
        int nanos = timestamp.getNanos();
        return (seconds == 0 ? 0 : 1 + CodedOutputStream.computeInt64SizeNoTag(seconds))
                + (nanos == 0 ? 0 : 1 + CodedOutputStream.computeInt32SizeNoTag(nanos));
    }

    /**
     * The length of a one-segment field path as the library encodes it: backslashes and backticks
     * escaped, and the whole segment backquoted unless it is a plain identifier.
     */
    private static long fieldPathLength(String segment) {
        long escapes = 0;
        boolean identifier = !segment.isEmpty();
        for (int i = 0; i < segment.length(); i++) {
            char c = segment.charAt(i);
            if (c == '\\' || c == '`') {
                escapes++;
            }
            boolean letter = c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
            if (!letter && (i == 0 || c < '0' || c > '9')) {
                identifier = false;
            }
        }
        long length = utf8Length(segment) + escapes;
        return identifier ? length : length + 2;
    }

    /** A length-delimited field with a one-byte tag around a body of the given size. */
    private static long delimited(long bodyLength) {
        return 1 + varintSize(bodyLength) + bodyLength;
    }

    private static long varintSize(long value) {
        return CodedOutputStream.computeUInt64SizeNoTag(value);
    }

    /**
     * The UTF-8 length of a string as protobuf encodes it, counted without encoding it. An unpaired
     * surrogate is not valid UTF-16, and protobuf then encodes the string through {@code
     * String.getBytes(UTF_8)}, which writes one replacement byte for it.
     */
    private static long utf8Length(String value) {
        long length = 0;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < 0x80) {
                length++;
            } else if (c < 0x800) {
                length += 2;
            } else if (Character.isHighSurrogate(c)
                    && i + 1 < value.length()
                    && Character.isLowSurrogate(value.charAt(i + 1))) {
                length += 4;
                i++;
            } else if (Character.isSurrogate(c)) {
                length++;
            } else {
                length += 3;
            }
        }
        return length;
    }
}
