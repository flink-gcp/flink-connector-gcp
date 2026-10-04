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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ExceptionUtils;

import com.google.api.gax.rpc.StatusCode;
import io.github.flink.gcp.connector.base.rpc.StatusCodes;

import java.util.EnumSet;
import java.util.Set;

/** Classifies whether a lookup's read failed transiently, so reading again may succeed. */
@Internal
final class FirestoreLookupErrorClassifier {

    /**
     * Statuses a lookup reads again on. They are exactly the statuses the client library retries
     * {@code BatchGetDocuments} on, which a document read goes through ({@code
     * FirestoreStubSettings}, {@code retry_policy_1_codes}, google-cloud-firestore 3.49.0). What
     * {@code lookup.max-retries} adds is budget on top of the library's, not a policy of the
     * connector's own, as on the Bigtable lookup (ADR-0095).
     *
     * <p>{@code RESOURCE_EXHAUSTED} is absent, as it is from the library's set for this RPC: the
     * loop re-reads at once, with no backoff, so including it would spend the whole budget against
     * a service that asked to be left alone.
     */
    private static final Set<StatusCode.Code> TRANSIENT_CODES =
            EnumSet.of(
                    StatusCode.Code.UNAVAILABLE,
                    StatusCode.Code.INTERNAL,
                    StatusCode.Code.DEADLINE_EXCEEDED);

    private FirestoreLookupErrorClassifier() {}

    static boolean isTransient(Throwable failure) {
        return ExceptionUtils.findThrowable(
                        failure,
                        candidate -> {
                            StatusCode.Code code = StatusCodes.codeOf(candidate);
                            return code != null && TRANSIENT_CODES.contains(code);
                        })
                .isPresent();
    }
}
