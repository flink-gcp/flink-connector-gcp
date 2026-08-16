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

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StatusCodes}. */
class StatusCodesTest {

    @Test
    void aGaxApiExceptionCarriesItsStatusCode() {
        ApiException exception = apiException(StatusCode.Code.NOT_FOUND);

        assertThat(StatusCodes.codeOf(exception)).isEqualTo(StatusCode.Code.NOT_FOUND);
    }

    @Test
    void aRawGrpcExceptionIsMappedByStatusName() {
        StatusRuntimeException exception = new StatusRuntimeException(Status.NOT_FOUND);

        assertThat(StatusCodes.codeOf(exception)).isEqualTo(StatusCode.Code.NOT_FOUND);
    }

    @Test
    void everyGrpcStatusCodeHasAGaxName() {
        // Pins the mapping assumption the raw-gRPC branch relies on: Code.valueOf(name) succeeds
        // for every status the transport can produce, so the unknown-name fallback is defensive.
        for (Status.Code code : Status.Code.values()) {
            assertThat(StatusCodes.codeOf(new StatusRuntimeException(code.toStatus())))
                    .isEqualTo(StatusCode.Code.valueOf(code.name()));
        }
    }

    @Test
    void onlyTheGivenThrowableIsInspectedNeverItsCause() {
        StatusRuntimeException cause = new StatusRuntimeException(Status.NOT_FOUND);
        ApiException wrapper = apiException(StatusCode.Code.INTERNAL, cause);

        assertThat(StatusCodes.codeOf(wrapper)).isEqualTo(StatusCode.Code.INTERNAL);
        assertThat(StatusCodes.codeOf(new RuntimeException(cause))).isNull();
    }

    @Test
    void aThrowableWithoutAStatusCarriesNone() {
        assertThat(StatusCodes.codeOf(new RuntimeException("boom"))).isNull();
        assertThat(StatusCodes.codeOf(new IllegalStateException())).isNull();
    }

    private static ApiException apiException(StatusCode.Code code) {
        return apiException(code, null);
    }

    private static ApiException apiException(StatusCode.Code code, Throwable cause) {
        StatusCode statusCode =
                new StatusCode() {
                    @Override
                    public Code getCode() {
                        return code;
                    }

                    @Override
                    public Object getTransportCode() {
                        return null;
                    }
                };
        return new ApiException(cause, statusCode, false);
    }
}
