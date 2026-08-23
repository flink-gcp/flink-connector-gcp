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

package io.github.flink.gcp.connector.cloudtasks.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ExceptionUtils;

import com.google.api.gax.rpc.StatusCode;
import io.github.flink.gcp.connector.base.rpc.StatusCodes;

import javax.annotation.Nullable;

import java.util.EnumSet;
import java.util.Set;

/** Classifies status codes reported by failed Cloud Tasks creations. */
@Internal
final class CloudTasksErrorClassifier {

    /** Statuses the sink retries on its main budget; a chain carrying one is never data-shaped. */
    private static final Set<StatusCode.Code> TRANSIENT_CODES =
            EnumSet.of(
                    StatusCode.Code.UNAVAILABLE,
                    StatusCode.Code.DEADLINE_EXCEEDED,
                    StatusCode.Code.RESOURCE_EXHAUSTED);

    private CloudTasksErrorClassifier() {}

    /**
     * Returns the status code the failure carries: the first element of the cause chain {@link
     * StatusCodes#codeOf(Throwable)} can classify.
     *
     * @param throwable the failure reported by the creation future
     * @return the outermost classifiable status, or {@code null} when the chain carries none
     */
    @Nullable
    static StatusCode.Code statusCode(Throwable throwable) {
        return firstMatching(throwable, null);
    }

    /**
     * Returns the first transient status anywhere in the cause chain.
     *
     * <p>This lookup deliberately scans past an outer non-transient status, so a failure carrying
     * service instability anywhere is never treated as data-shaped.
     *
     * @param throwable the failure reported by the creation future
     * @return the first transient status, or {@code null} when the chain carries none
     */
    @Nullable
    static StatusCode.Code transientCode(Throwable throwable) {
        return firstMatching(throwable, TRANSIENT_CODES);
    }

    /**
     * Returns the first status code in the cause chain that is one of {@code codes}, or {@code
     * null} when the chain carries none; a null {@code codes} accepts any classifiable status.
     */
    @Nullable
    private static StatusCode.Code firstMatching(
            Throwable throwable, @Nullable Set<StatusCode.Code> codes) {
        return ExceptionUtils.findThrowable(
                        throwable,
                        t -> {
                            StatusCode.Code code = StatusCodes.codeOf(t);
                            return code != null && (codes == null || codes.contains(code));
                        })
                .map(StatusCodes::codeOf)
                .orElse(null);
    }
}
