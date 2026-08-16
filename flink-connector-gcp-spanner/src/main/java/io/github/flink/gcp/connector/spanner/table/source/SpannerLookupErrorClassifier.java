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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.util.ExceptionUtils;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;

/** Classifies point-read failures eligible for another client call. */
final class SpannerLookupErrorClassifier {
    private SpannerLookupErrorClassifier() {}

    static boolean isTransient(Throwable failure) {
        return ExceptionUtils.findThrowable(failure, SpannerException.class)
                .map(SpannerException::getErrorCode)
                .map(
                        code ->
                                code == ErrorCode.ABORTED
                                        || code == ErrorCode.DEADLINE_EXCEEDED
                                        || code == ErrorCode.UNAVAILABLE)
                .orElse(false);
    }
}
