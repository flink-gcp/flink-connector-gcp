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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.options.ResourceNames;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;

import java.io.Serializable;
import java.util.Objects;

/**
 * Settings for checkpoint-owned writes. Writer limits bound one checkpoint interval, not total
 * committer heap. The marker family must be provisioned separately, raw and without a GC rule.
 */
@Experimental
public final class BigtableStagedOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String markerFamily;
    private final int maxStagedEntries;
    private final long maxStagedBytes;
    private final BigtableRequestOptions requestOptions;

    private BigtableStagedOptions(Builder builder) {
        markerFamily = builder.markerFamily;
        maxStagedEntries = builder.maxStagedEntries;
        maxStagedBytes = builder.maxStagedBytes;
        requestOptions = builder.requestOptions;
        validate();
    }

    /** Creates a new builder; the marker family has no implicit name. */
    public static Builder builder() {
        return new Builder();
    }

    /** Returns the reserved family whose markers must survive every supported restore. */
    public String getMarkerFamily() {
        return markerFamily;
    }

    /** Returns the maximum number of entries staged between prepareCommit calls. */
    public int getMaxStagedEntries() {
        return maxStagedEntries;
    }

    /** Returns the maximum serialized request bytes plus 256 accounting bytes per staged entry. */
    public long getMaxStagedBytes() {
        return maxStagedBytes;
    }

    /** Returns the single-attempt RPC and bounded client settings used by the committer. */
    public BigtableRequestOptions getRequestOptions() {
        return requestOptions;
    }

    /** Revalidates serialized job configuration before creating runtime components. */
    public void validate() {
        ResourceNames.checkNotBlank(markerFamily, "markerFamily");
        Preconditions.checkArgument(maxStagedEntries > 0, "maxStagedEntries must be positive");
        Preconditions.checkArgument(maxStagedBytes > 0, "maxStagedBytes must be positive");
        Preconditions.checkNotNull(requestOptions, "requestOptions");
        BigtableRequestOptions.builder()
                .maxInFlightRequests(requestOptions.getMaxInFlightRequests())
                .requestTimeout(requestOptions.getRequestTimeout())
                .destinationIdleTimeout(requestOptions.getDestinationIdleTimeout())
                .maxActiveInstances(requestOptions.getMaxActiveInstances())
                .perDestinationMetrics(requestOptions.isPerDestinationMetrics())
                .build();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof BigtableStagedOptions)) {
            return false;
        }
        BigtableStagedOptions that = (BigtableStagedOptions) other;
        return markerFamily.equals(that.markerFamily)
                && maxStagedEntries == that.maxStagedEntries
                && maxStagedBytes == that.maxStagedBytes
                && requestOptions.equals(that.requestOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(markerFamily, maxStagedEntries, maxStagedBytes, requestOptions);
    }

    /** Builds an immutable staged configuration. */
    @Experimental
    public static final class Builder {
        private String markerFamily;
        private int maxStagedEntries = 100_000;
        private long maxStagedBytes = 64L * 1024 * 1024;
        private BigtableRequestOptions requestOptions = BigtableRequestOptions.builder().build();

        private Builder() {}

        /** Selects the operator-provisioned raw family with no GC rule. */
        public Builder markerFamily(String value) {
            markerFamily = ResourceNames.checkNotBlank(value, "markerFamily");
            return this;
        }

        /** Sets the positive entry cap for a writer's checkpoint interval. */
        public Builder maxStagedEntries(int value) {
            Preconditions.checkArgument(value > 0, "maxStagedEntries must be positive");
            maxStagedEntries = value;
            return this;
        }

        /** Sets the positive serialized-byte cap, including the per-entry accounting allowance. */
        public Builder maxStagedBytes(long value) {
            Preconditions.checkArgument(value > 0, "maxStagedBytes must be positive");
            maxStagedBytes = value;
            return this;
        }

        /** Sets the committer's single-row RPC and client settings. */
        public Builder requestOptions(BigtableRequestOptions value) {
            requestOptions = Preconditions.checkNotNull(value, "requestOptions");
            return this;
        }

        /** Builds the settings, requiring an explicit marker family. */
        public BigtableStagedOptions build() {
            return new BigtableStagedOptions(this);
        }
    }
}
