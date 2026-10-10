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

package io.github.flink.gcp.connector.datastore.table.source;

import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.datastore.v1.stub.DatastoreStubSettings;
import com.google.datastore.v1.Key;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.EnumSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class DatastoreLookupErrorClassifierTest {

    @ParameterizedTest
    @EnumSource(
            value = StatusCode.Code.class,
            names = {"UNAVAILABLE", "DEADLINE_EXCEEDED"})
    void theLibrarysRetryableStatusesAreTransient(StatusCode.Code code) {
        assertThat(DatastoreLookupErrorClassifier.isTransient(FakeEntityLookup.failure(code)))
                .isTrue();
        assertThat(
                        DatastoreLookupErrorClassifier.isTransient(
                                new IllegalStateException(
                                        "wrapped", FakeEntityLookup.failure(code))))
                .as("found through the cause chain")
                .isTrue();
    }

    @ParameterizedTest
    @EnumSource(
            value = StatusCode.Code.class,
            mode = EnumSource.Mode.EXCLUDE,
            names = {"UNAVAILABLE", "DEADLINE_EXCEEDED"})
    void everyOtherStatusIsPermanent(StatusCode.Code code) {
        assertThat(DatastoreLookupErrorClassifier.isTransient(FakeEntityLookup.failure(code)))
                .isFalse();
    }

    @Test
    void aDeferralIsTransient() {
        RuntimeException deferral =
                new DatastoreEntityLookups.DeferredException(
                        Key.newBuilder()
                                .addPath(Key.PathElement.newBuilder().setKind("K").setName("a"))
                                .build());

        assertThat(DatastoreLookupErrorClassifier.isTransient(deferral)).isTrue();
        assertThat(DatastoreLookupErrorClassifier.isTransient(new RuntimeException(deferral)))
                .isTrue();
    }

    @Test
    void aFailureWithoutAStatusIsPermanent() {
        assertThat(
                        DatastoreLookupErrorClassifier.isTransient(
                                new IllegalStateException("no status")))
                .isFalse();
    }

    @Test
    void theStatusesAreTheOnesTheLibraryRetriesLookupOn() {
        // A tripwire for a libraries-bom bump: the classifier adds budget on top of the
        // library's own retries, so the two sets must stay one.
        Set<StatusCode.Code> library =
                EnumSet.copyOf(
                        DatastoreStubSettings.newBuilder().lookupSettings().getRetryableCodes());
        Set<StatusCode.Code> classified = EnumSet.noneOf(StatusCode.Code.class);
        for (StatusCode.Code code : StatusCode.Code.values()) {
            if (DatastoreLookupErrorClassifier.isTransient(FakeEntityLookup.failure(code))) {
                classified.add(code);
            }
        }
        assertThat(classified).isEqualTo(library);
    }
}
