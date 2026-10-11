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

import com.google.datastore.v1.Mutation;
import com.google.protobuf.CodedOutputStream;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;

import java.nio.charset.StandardCharsets;

/**
 * The size a mutation adds to a {@code Commit} request on the wire, which the writer bounds a batch
 * by, and the size of the request's own fields.
 *
 * <p>A mutation's size is that of the protobuf the writer commits ({@link EncodedMutation}),
 * property mask included, framed as the request's {@code mutations} field.
 */
@Internal
final class MutationSizeEstimator {

    private MutationSizeEstimator() {}

    /**
     * Returns the bytes the encoded mutation adds to a {@code Commit} request.
     *
     * @param mutation the mutation as the request carries it
     * @return its size on the wire, framing included
     */
    static long sizeOf(Mutation mutation) {
        return framed(mutation.getSerializedSize());
    }

    /**
     * Returns the bytes a {@code Commit} request to the database takes besides its mutations: the
     * project id, the database id when it is not the default's empty one, and the mode, which
     * {@code NON_TRANSACTIONAL} encodes in two bytes.
     *
     * @param database the database the request names
     * @return the size of the request's other fields
     */
    static long requestHeaderSize(DatabaseDestination database) {
        long size = framed(utf8Length(database.getProject())) + 2;
        if (!database.getDatabaseId().isEmpty()) {
            size += framed(utf8Length(database.getDatabaseId()));
        }
        return size;
    }

    private static long utf8Length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    /** The bytes a length-delimited field of the given length takes, with a one-byte tag. */
    private static long framed(long length) {
        return 1 + CodedOutputStream.computeUInt64SizeNoTag(length) + length;
    }
}
