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

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FirestoreDocumentReferenceTest {

    @Test
    void carriesItsPathAndComparesByIt() throws Exception {
        FirestoreDocumentReference reference = FirestoreDocumentReference.of("users/alice");

        assertThat(reference.getDocumentPath()).isEqualTo("users/alice");
        assertThat(reference)
                .isEqualTo(FirestoreDocumentReference.of("users/alice"))
                .hasSameHashCodeAs(FirestoreDocumentReference.of("users/alice"))
                .isNotEqualTo(FirestoreDocumentReference.of("users/bob"));
        assertThat(InstantiationUtil.clone(reference)).isEqualTo(reference);
        assertThat(reference.toString()).contains("users/alice");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "users", "users/alice/orders", "/users/alice", "users//alice/x"})
    void aMalformedPathIsRejectedAsADocumentPathIs(String path) {
        assertThatThrownBy(() -> FirestoreDocumentReference.of(path))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Document path");
    }

    @Test
    void aNullPathIsRejected() {
        assertThatThrownBy(() -> FirestoreDocumentReference.of(null))
                .isInstanceOf(NullPointerException.class);
    }
}
