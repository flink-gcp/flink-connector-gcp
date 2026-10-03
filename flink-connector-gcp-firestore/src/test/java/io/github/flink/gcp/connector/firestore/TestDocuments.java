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

package io.github.flink.gcp.connector.firestore;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.Internal;
import com.google.cloud.firestore.spi.v1.GrpcFirestoreRpc;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.Value;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

/**
 * Mints document snapshots, for tests of the Firestore source that must not run a read.
 *
 * <p>A {@link DocumentSnapshot} has no public constructor. The client library's {@link Internal}
 * builds one from a {@code Document} proto, the way a read does; it is public but marked
 * {@code @InternalApi}, so a {@code libraries-bom} bump that changes it fails these tests at
 * compile time. No helper in the vendor's package is needed (ADR-0067's bar is not met: a public
 * factory exists). The client never connects: it points at an emulator address nothing listens on.
 */
public final class TestDocuments {

    private TestDocuments() {}

    /**
     * Returns a client that is never connected to, for building queries offline.
     *
     * @param project the project id
     * @return the client, which the caller closes
     */
    public static Firestore offlineClient(String project) {
        return options(project).getService();
    }

    /**
     * Returns a snapshot of one document as a query read would produce it.
     *
     * @param client the client whose database the document belongs to
     * @param path the document path, relative to the database's documents root
     * @param fields the document's fields: {@code String} and {@code Long} values, or a {@link
     *     Value} stored as it is, so that a test states the wire form without the client library
     * @param readTime the read time the snapshot carries
     * @return the snapshot
     */
    public static DocumentSnapshot document(
            Firestore client, String path, Map<String, Object> fields, Timestamp readTime) {
        FirestoreOptions options = client.getOptions();
        Document.Builder document =
                Document.newBuilder()
                        .setName(
                                "projects/"
                                        + options.getProjectId()
                                        + "/databases/"
                                        + options.getDatabaseId()
                                        + "/documents/"
                                        + path)
                        .setCreateTime(readTime.toProto())
                        .setUpdateTime(readTime.toProto());
        fields.forEach((name, value) -> document.putFields(name, encode(value)));
        try (GrpcFirestoreRpc rpc = new GrpcFirestoreRpc(options)) {
            return new Internal(options, rpc).snapshotFromProto(readTime, document.build());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static FirestoreOptions options(String project) {
        return FirestoreOptions.newBuilder()
                .setProjectId(project)
                .setEmulatorHost("localhost:1")
                .build();
    }

    private static Value encode(Object value) {
        if (value instanceof Value) {
            return (Value) value;
        }
        if (value instanceof String) {
            return Value.newBuilder().setStringValue((String) value).build();
        }
        if (value instanceof Long) {
            return Value.newBuilder().setIntegerValue((Long) value).build();
        }
        throw new IllegalArgumentException(
                "Only String, Long and Value fields are supported: " + value);
    }
}
