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

import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.PathElement;
import com.google.protobuf.TextFormat;
import org.junit.jupiter.api.Test;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class DatastoreServiceAdapterTest {

    /**
     * The lookup encodes its key itself; the client library's own encoding is reachable only as
     * {@code Key.toUrlSafe()}, a URL-encoded protobuf text form, which this decodes to compare.
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

            assertThat(DatastoreServiceAdapter.toProto(key))
                    .as(key.toString())
                    .isEqualTo(expected.build());
        }
    }
}
