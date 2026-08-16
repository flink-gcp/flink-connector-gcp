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
