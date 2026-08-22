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

package io.github.flink.gcp.connector.bigtable.table.source;

import org.apache.flink.util.ExceptionUtils;

import com.google.api.gax.rpc.StatusCode;
import io.github.flink.gcp.connector.base.rpc.StatusCodes;

import java.util.EnumSet;
import java.util.Set;

/** Classifies whether a point-read failure is transient and eligible for another client call. */
final class BigtableLookupErrorClassifier {

    /**
     * Statuses a point read is issued again on. They are exactly the statuses the client itself
     * retries {@code ReadRow} on, and they are listed in the order its own {@code
     * EnhancedBigtableStubSettings.readRowSettings()} documentation lists them (checked against
     * google-cloud-bigtable 2.81.0). What {@code lookup.max-retries} adds is therefore budget on
     * top of the vendor's, not a policy of the connector's own.
     *
     * <p>{@code RESOURCE_EXHAUSTED} is absent, which is the opposite decision from the constant of
     * the same name in {@code BigtableErrorClassifier}, where it is present. The reason is not that
     * one of the two has fallen behind the other. The two sets gate opposite actions on the same
     * status: the writer's decides that a failure is never the record's fault, so a mutation
     * refused for capacity is never dead-lettered, while this one decides that a call is worth
     * issuing again. Bigtable documents the status as an exhausted project-level admin API quota, a
     * node limit or a node storage limit, and another point read clears none of the three. Because
     * the client does not retry the status either, each attempt here returns without the client's
     * backoff in front of it, and this loop re-issues immediately, so including it would spend the
     * whole budget in quick succession against a service already refusing for capacity.
     */
    private static final Set<StatusCode.Code> TRANSIENT_CODES =
            EnumSet.of(
                    StatusCode.Code.DEADLINE_EXCEEDED,
                    StatusCode.Code.UNAVAILABLE,
                    StatusCode.Code.ABORTED);

    private BigtableLookupErrorClassifier() {}

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
