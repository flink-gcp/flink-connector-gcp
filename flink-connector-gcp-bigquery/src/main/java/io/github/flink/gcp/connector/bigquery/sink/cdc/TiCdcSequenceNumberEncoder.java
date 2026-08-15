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

package io.github.flink.gcp.connector.bigquery.sink.cdc;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.io.Serializable;

import static java.util.Objects.requireNonNull;

/** Transcodes a TiCDC commit TSO into a BigQuery CDC sequence section. */
@Internal
public final class TiCdcSequenceNumberEncoder implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String CONNECTOR = "TiCDC";

    private final String clusterId;

    /** Creates an encoder bound to one TiDB cluster identity. */
    public TiCdcSequenceNumberEncoder(String clusterId) {
        requireNonNull(clusterId, "clusterId must not be null");
        if (clusterId.isEmpty()) {
            throw new IllegalArgumentException(
                    "TiCDC sequence generation requires a non-empty cluster ID");
        }
        this.clusterId = clusterId;
    }

    /** Encodes the TiCDC source properties used by the built-in profile. */
    public String sequenceNumber(
            @Nullable String connector,
            @Nullable String snapshot,
            @Nullable String commitTs,
            @Nullable String clusterId) {
        if (!CONNECTOR.equals(connector)) {
            throw new IllegalArgumentException(
                    "Expected connector '" + CONNECTOR + "' but found '" + connector + "'");
        }
        if (snapshot != null && !"false".equals(snapshot)) {
            throw new IllegalArgumentException(
                    "TiCDC row changes always carry 'snapshot' 'false' but found '"
                            + snapshot
                            + "'");
        }
        if (clusterId == null || clusterId.isEmpty()) {
            throw new IllegalArgumentException(
                    "TiCDC sequence generation requires a non-empty 'cluster_id'");
        }
        if (!this.clusterId.equals(clusterId)) {
            throw new IllegalArgumentException(
                    "TiCDC event belongs to cluster '"
                            + clusterId
                            + "' but this sink orders cluster '"
                            + this.clusterId
                            + "'");
        }
        return CdcSequenceNumberSections.format(parseCommitTs(commitTs));
    }

    private static long parseCommitTs(@Nullable String commitTs) {
        long value;
        try {
            value = CdcSequenceNumberSections.parseUnsignedDecimal(commitTs);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "TiCDC 'commit_ts' must be an unsigned 64-bit decimal value but found '"
                            + commitTs
                            + "'");
        }
        if (value == 0L) {
            throw new IllegalArgumentException(
                    "TiCDC 'commit_ts' must be a positive timestamp oracle value");
        }
        return value;
    }
}
