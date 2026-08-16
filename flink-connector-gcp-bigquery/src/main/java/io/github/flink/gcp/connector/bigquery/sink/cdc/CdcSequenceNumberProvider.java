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

package io.github.flink.gcp.connector.bigquery.sink.cdc;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/** Extracts the BigQuery change sequence number for one record. */
@FunctionalInterface
@PublicEvolving
public interface CdcSequenceNumberProvider<T> extends Serializable {

    /**
     * Returns one to four slash-separated hexadecimal sections, each containing at most 16 digits.
     */
    String getSequenceNumber(T element);
}
