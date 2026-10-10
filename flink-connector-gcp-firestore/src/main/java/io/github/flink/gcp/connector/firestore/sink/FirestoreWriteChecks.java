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

package io.github.flink.gcp.connector.firestore.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Blob;
import com.google.cloud.firestore.GeoPoint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The checks {@link FirestoreWrite} applies when it is built: the document or collection path's
 * grammar and the closed value vocabulary.
 *
 * <p>Both exist for what the sink does with the value, not for what the service might refuse
 * (ADR-0127). The path is parsed here and composed into a document reference, so it is checked
 * against the grammar that reads it. The values are handed to the client library, which fails
 * synchronously on a value it cannot encode in a way that corrupts the rest of its request
 * (ADR-0171), so a value is accepted only if the library is known to encode it. The one exception
 * is {@link FirestoreDocumentReference}, which the library never sees: the writer replaces it with
 * the library's own reference before it hands the fields over, and the library would otherwise
 * encode it as a map of its properties.
 *
 * <p>A field's path is built only for an error message, or once per nested map or list: the copy
 * runs for every record, and most values are scalars that need none.
 */
@Internal
final class FirestoreWriteChecks {

    private FirestoreWriteChecks() {}

    /**
     * Checks a document path relative to the documents root: an even number of {@code /}-separated
     * segments, none of them empty.
     */
    static void checkDocumentPath(String documentPath) {
        Preconditions.checkNotNull(documentPath, "documentPath must not be null");
        int segments = checkSegments("Document", documentPath);
        if (segments % 2 != 0) {
            throw new IllegalArgumentException(
                    "Document path '"
                            + documentPath
                            + "' has "
                            + segments
                            + " segment(s). A document path alternates collection and document"
                            + " ids, so it has an even number of segments, for example"
                            + " 'users/alice'.");
        }
    }

    /**
     * Checks a collection path relative to the documents root: an odd number of {@code /}-separated
     * segments, none of them empty. The document path an id is appended to must be one {@link
     * #checkDocumentPath} accepts.
     */
    static void checkCollectionPath(String collectionPath) {
        Preconditions.checkNotNull(collectionPath, "collectionPath must not be null");
        int segments = checkSegments("Collection", collectionPath);
        if (segments % 2 == 0) {
            throw new IllegalArgumentException(
                    "Collection path '"
                            + collectionPath
                            + "' has "
                            + segments
                            + " segment(s). A collection path alternates collection and document"
                            + " ids and ends with a collection id, so it has an odd number of"
                            + " segments, for example 'users' or 'users/alice/orders'.");
        }
    }

    /** Refuses an empty segment in a path, and returns how many segments it has. */
    private static int checkSegments(String kind, String path) {
        String[] segments = path.split("/", -1);
        for (String segment : segments) {
            if (segment.isEmpty()) {
                throw new IllegalArgumentException(
                        kind
                                + " path '"
                                + path
                                + "' has an empty segment. A path alternates collection and"
                                + " document ids separated by single '/' characters, with no"
                                + " leading or trailing '/'.");
            }
        }
        return segments.length;
    }

    /** Checks and copies a document's fields into unmodifiable, serializable collections. */
    static Map<String, Object> copyFields(Map<String, ?> fields) {
        return copyMap(fields, "", 0);
    }

    /**
     * Copies a map whose own path is {@code path} (empty for the document) at nesting {@code
     * depth}.
     */
    private static Map<String, Object> copyMap(Map<?, ?> map, String path, int depth) {
        Map<String, Object> copy = new LinkedHashMap<>(map.size() * 2);
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Object key = entry.getKey();
            if (!(key instanceof String) || ((String) key).isEmpty()) {
                throw new IllegalArgumentException(
                        "Field names must be non-empty strings, but "
                                + (path.isEmpty() ? "the document" : "field '" + path + "'")
                                + " has the name "
                                + (key == null ? "null" : "'" + key + "'")
                                + ".");
            }
            String name = (String) key;
            copy.put(name, copyValue(entry.getValue(), path, name, -1, depth + 1));
        }
        return Collections.unmodifiableMap(copy);
    }

    /**
     * Copies one value: the field {@code name} of the container at {@code parent}, or its element
     * {@code index} when {@code name} is {@code null}.
     */
    private static Object copyValue(
            Object value, String parent, String name, int index, int depth) {
        if (depth > FirestoreWrite.MAX_NESTING_DEPTH) {
            throw new IllegalArgumentException(
                    "Field '"
                            + childPath(parent, name, index)
                            + "' is nested deeper than "
                            + FirestoreWrite.MAX_NESTING_DEPTH
                            + " levels.");
        }
        if (value instanceof Blob && ((Blob) value).subtype() != 0) {
            // google-cloud-firestore 3.49.0 encodes a BSON binary of any other subtype as a
            // reserved map rather than bytes, which the writer's request-size accounting does not
            // count (#1589).
            throw new IllegalArgumentException(
                    "Field '"
                            + childPath(parent, name, index)
                            + "' is a BSON binary Blob of subtype "
                            + ((Blob) value).subtype()
                            + ", which FirestoreWrite does not accept, because only subtype 0 is"
                            + " encoded as bytes. Write bytes as Blob.fromBytes(...).");
        }
        if (value == null
                || value instanceof String
                || value instanceof Long
                || value instanceof Double
                || value instanceof Boolean
                || value instanceof Timestamp
                || value instanceof GeoPoint
                || value instanceof Blob
                || value instanceof FirestoreDocumentReference) {
            return value;
        }
        if (value instanceof Map) {
            return copyMap((Map<?, ?>) value, childPath(parent, name, index), depth);
        }
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            String path = childPath(parent, name, index);
            List<Object> copy = new ArrayList<>(list.size());
            for (int i = 0; i < list.size(); i++) {
                copy.add(copyValue(list.get(i), path, null, i, depth + 1));
            }
            return Collections.unmodifiableList(copy);
        }
        throw new IllegalArgumentException(
                "Field '"
                        + childPath(parent, name, index)
                        + "' has a value of type "
                        + value.getClass().getName()
                        + ", which a Firestore write does not accept. Use String, Long, Double,"
                        + " Boolean, com.google.cloud.Timestamp, GeoPoint, Blob,"
                        + " FirestoreDocumentReference, a List or a Map with String keys: an int"
                        + " is written as a Long, a float as a Double, bytes as"
                        + " Blob.fromBytes(...), and a reference to a document as"
                        + " FirestoreDocumentReference.of(path).");
    }

    private static String childPath(String parent, String name, int index) {
        if (name == null) {
            return parent + "[" + index + "]";
        }
        return parent.isEmpty() ? name : parent + "." + name;
    }
}
