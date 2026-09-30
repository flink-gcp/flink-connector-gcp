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

import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.firestore.BulkWriter;
import com.google.cloud.firestore.v1.stub.FirestoreStubSettings;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWriterOptions;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static io.github.flink.gcp.connector.firestore.sink.writer.FakeFirestoreDatabaseAccess.failure;
import static org.assertj.core.api.Assertions.assertThat;

class BulkWriterRetryPolicyTest {

    @Test
    void theRetryableCodesAreTheLibrarysBatchWriteCodes() {
        Set<StatusCode.Code> library =
                FirestoreStubSettings.newBuilder().batchWriteSettings().getRetryableCodes();

        assertThat(
                        BulkWriterRetryPolicy.RETRYABLE_CODES.stream()
                                .map(code -> StatusCode.Code.valueOf(code.name()))
                                .collect(Collectors.toSet()))
                .isEqualTo(library);
    }

    @Test
    void theDefaultBudgetIsTheLibrarysOwn() {
        BulkWriterRetryPolicy policy =
                new BulkWriterRetryPolicy(
                        FirestoreWriterOptions.DEFAULT_WRITE_MAX_ATTEMPTS, new Counting());

        // The library's default listener stops once failedAttempts exceeds MAX_RETRY_ATTEMPTS.
        assertThat(policy.shouldRetry(Status.Code.UNAVAILABLE, BulkWriter.MAX_RETRY_ATTEMPTS))
                .isTrue();
        assertThat(policy.shouldRetry(Status.Code.UNAVAILABLE, BulkWriter.MAX_RETRY_ATTEMPTS + 1))
                .isFalse();
    }

    @Test
    void oneAttemptMeansNoRetry() {
        BulkWriterRetryPolicy policy = new BulkWriterRetryPolicy(1, new Counting());

        assertThat(policy.shouldRetry(Status.Code.UNAVAILABLE, 1)).isFalse();
    }

    @Test
    void theObserverHearsEveryFailedAttemptAndEveryRetry() {
        Counting observer = new Counting();
        BulkWriterRetryPolicy policy = new BulkWriterRetryPolicy(3, observer);

        assertThat(policy.onError(failure(Status.ABORTED))).isTrue();
        assertThat(policy.onError(failure(Status.INVALID_ARGUMENT))).isFalse();

        assertThat(observer.failed).isEqualTo(2);
        assertThat(observer.retried).isEqualTo(1);
    }

    private static final class Counting implements BulkWriterRetryPolicy.Observer {
        int failed;
        int retried;

        @Override
        public void attemptFailed() {
            failed++;
        }

        @Override
        public void retrying() {
            retried++;
        }
    }
}
