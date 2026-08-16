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

package io.github.flink.gcp.connector.bigquery.sink.serializer;

import org.apache.flink.annotation.Public;

/**
 * Supported BigQuery types for singular additional physical fields.
 *
 * <p>The value returned by a field provider must have the Java type documented on the enum
 * constant. {@link #TIMESTAMP} uses BigQuery's microsecond-precision representation.
 */
@Public
public enum AdditionalFieldType {
    /** A {@link Boolean}. */
    BOOL,

    /** A protobuf {@link com.google.protobuf.ByteString}. */
    BYTES,

    /** A {@link java.time.LocalDate}. */
    DATE,

    /** A {@link java.time.LocalDateTime}. */
    DATETIME,

    /** A {@link Double}. */
    DOUBLE,

    /** A geography value represented as a {@link String}. */
    GEOGRAPHY,

    /** A {@link Long}. */
    INT64,

    /** A {@link java.math.BigDecimal} within BigQuery {@code NUMERIC} bounds. */
    NUMERIC,

    /** A {@link java.math.BigDecimal} within BigQuery {@code BIGNUMERIC} bounds. */
    BIGNUMERIC,

    /** A {@link String}. */
    STRING,

    /** A {@link java.time.LocalTime}. */
    TIME,

    /** A microsecond-precision {@link java.time.Instant}. */
    TIMESTAMP,

    /** JSON text represented as a {@link String}. */
    JSON
}
