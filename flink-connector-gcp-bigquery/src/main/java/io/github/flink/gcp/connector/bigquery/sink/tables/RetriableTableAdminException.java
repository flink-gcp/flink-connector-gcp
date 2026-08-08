/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigquery.sink.tables;

import org.apache.flink.annotation.Internal;

import java.io.IOException;

/**
 * A {@link TableAdmin} failure that repeating the same call can fix — a rate limit, a quota that
 * refills, a server-side error. Every other {@link IOException} the SPI throws is terminal.
 *
 * <p>A type rather than a predicate over the cause chain, because the type is what keeps the REST
 * client's {@code BigQueryException} inside this package: {@link TableAdmin} exists to abstract
 * that client away from the writers, and a caller that had to reach through the SPI for a vendor
 * exception would be undoing the abstraction it is written against.
 *
 * <p>Public because the callers live in a sibling package (the storage writers), for the same
 * reason {@code AppendErrorClassifier.isExistenceMasked} is.
 */
@Internal
public final class RetriableTableAdminException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a retriable failure.
     *
     * @param message what failed
     * @param cause the failure from the client
     */
    public RetriableTableAdminException(String message, Throwable cause) {
        super(message, cause);
    }
}
