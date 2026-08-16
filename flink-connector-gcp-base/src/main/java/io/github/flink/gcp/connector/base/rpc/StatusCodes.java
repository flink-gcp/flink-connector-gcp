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

package io.github.flink.gcp.connector.base.rpc;

import org.apache.flink.annotation.Internal;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import io.grpc.StatusRuntimeException;

import javax.annotation.Nullable;

/** Extraction of gRPC status codes from the exceptions the Google Cloud clients surface. */
@Internal
public final class StatusCodes {

    private StatusCodes() {}

    /**
     * Returns the status code the given throwable itself carries — from the gax {@link
     * ApiException} the client libraries surface, or from a raw gRPC {@link StatusRuntimeException}
     * (defense in depth) — or {@code null} when it carries none.
     *
     * <p>Only the given throwable is inspected. Walking a cause chain, and deciding which element
     * of it classifies the failure, stays with the caller (typically {@code
     * ExceptionUtils.findThrowable}): retryability classification is per-connector policy.
     */
    @Nullable
    public static StatusCode.Code codeOf(Throwable throwable) {
        if (throwable instanceof ApiException) {
            return ((ApiException) throwable).getStatusCode().getCode();
        }
        if (throwable instanceof StatusRuntimeException) {
            // The two enums name the same gRPC status codes; an unknown name means an
            // unclassifiable failure rather than a crash.
            try {
                return StatusCode.Code.valueOf(
                        ((StatusRuntimeException) throwable).getStatus().getCode().name());
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
        return null;
    }
}
