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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ExceptionUtils;

import com.google.api.gax.rpc.StatusCode;
import io.github.flink.gcp.connector.base.rpc.StatusCodes;

import java.util.EnumSet;
import java.util.Set;

/** Classifies whether a lookup's read failed transiently, so reading again may succeed. */
@Internal
final class DatastoreLookupErrorClassifier {

    /**
     * Statuses a lookup reads again on. They are exactly the statuses the generated client retries
     * {@code Lookup} on ({@code DatastoreStubSettings}, {@code retry_policy_0_codes},
     * google-cloud-datastore 3.7.0). What {@code lookup.max-retries} adds is budget on top of the
     * library's, not a policy of the connector's own, as on the Native-mode lookup (ADR-0179).
     *
     * <p>{@code INTERNAL}, which the library retries for Native mode's {@code BatchGetDocuments},
     * is absent, as it is from the library's set for this RPC; so is {@code RESOURCE_EXHAUSTED}:
     * the loop re-reads at once, with no backoff, so including it would spend the whole budget
     * against a service that asked to be left alone.
     *
     * <p>A deferral of the key is transient too, although it also reports resource constraints: it
     * is a key the service did not read, which a miss would hide (ADR-0184). It is sent again at
     * once, like the statuses above, so a service deferring the key for longer than the budget's
     * immediate re-sends fails the join.
     */
    private static final Set<StatusCode.Code> TRANSIENT_CODES =
            EnumSet.of(StatusCode.Code.UNAVAILABLE, StatusCode.Code.DEADLINE_EXCEEDED);

    private DatastoreLookupErrorClassifier() {}

    /**
     * Returns whether the failure, anywhere in its cause chain, carries one of those statuses or is
     * a deferral of the key.
     */
    static boolean isTransient(Throwable failure) {
        return ExceptionUtils.findThrowable(
                        failure,
                        candidate -> {
                            if (candidate instanceof DatastoreEntityLookups.DeferredException) {
                                return true;
                            }
                            StatusCode.Code code = StatusCodes.codeOf(candidate);
                            return code != null && TRANSIENT_CODES.contains(code);
                        })
                .isPresent();
    }
}
