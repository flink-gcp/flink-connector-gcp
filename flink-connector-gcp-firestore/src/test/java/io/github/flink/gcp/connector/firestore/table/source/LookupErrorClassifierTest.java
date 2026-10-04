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

package io.github.flink.gcp.connector.firestore.table.source;

import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.firestore.v1.stub.FirestoreStubSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.EnumSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class LookupErrorClassifierTest {

    @ParameterizedTest
    @EnumSource(
            value = StatusCode.Code.class,
            names = {"UNAVAILABLE", "INTERNAL", "DEADLINE_EXCEEDED"})
    void theLibrarysRetryableStatusesAreTransient(StatusCode.Code code) {
        assertThat(LookupErrorClassifier.isTransient(FakeDocumentLookup.failure(code))).isTrue();
        assertThat(
                        LookupErrorClassifier.isTransient(
                                new IllegalStateException(
                                        "wrapped", FakeDocumentLookup.failure(code))))
                .as("found through the cause chain")
                .isTrue();
    }

    @ParameterizedTest
    @EnumSource(
            value = StatusCode.Code.class,
            mode = EnumSource.Mode.EXCLUDE,
            names = {"UNAVAILABLE", "INTERNAL", "DEADLINE_EXCEEDED"})
    void everyOtherStatusIsPermanent(StatusCode.Code code) {
        assertThat(LookupErrorClassifier.isTransient(FakeDocumentLookup.failure(code))).isFalse();
    }

    @Test
    void aFailureWithoutAStatusIsPermanent() {
        assertThat(LookupErrorClassifier.isTransient(new IllegalStateException("no status")))
                .isFalse();
    }

    @Test
    void theSetIsTheOneTheLibraryRetriesBatchGetDocumentsOn() {
        // A tripwire for a libraries-bom bump: the classifier adds budget on top of the
        // library's own retries, so the two sets must stay one.
        Set<StatusCode.Code> library =
                EnumSet.copyOf(
                        FirestoreStubSettings.newBuilder()
                                .batchGetDocumentsSettings()
                                .getRetryableCodes());
        Set<StatusCode.Code> classified = EnumSet.noneOf(StatusCode.Code.class);
        for (StatusCode.Code code : StatusCode.Code.values()) {
            if (LookupErrorClassifier.isTransient(FakeDocumentLookup.failure(code))) {
                classified.add(code);
            }
        }
        assertThat(classified).isEqualTo(library);
    }
}
