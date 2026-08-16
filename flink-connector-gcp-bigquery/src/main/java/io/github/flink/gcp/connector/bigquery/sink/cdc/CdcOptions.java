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

import org.apache.flink.annotation.Public;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Options for appending BigQuery CDC mutations through the Storage Write API default stream.
 *
 * <p>The change type provider is required. A sequence provider is optional; when configured, it
 * must return a valid non-null sequence for every record the configured serializer does not skip.
 * Instances are immutable and serializable.
 */
@Public
public final class CdcOptions<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final CdcChangeTypeProvider<? super T> changeTypeProvider;
    @Nullable private final CdcSequenceNumberProvider<? super T> sequenceNumberProvider;

    private CdcOptions(Builder<T> builder) {
        this.changeTypeProvider = builder.changeTypeProvider;
        this.sequenceNumberProvider = builder.sequenceNumberProvider;
    }

    /** Creates a builder with the required change type provider. */
    public static <T> Builder<T> builder(CdcChangeTypeProvider<? super T> changeTypeProvider) {
        return new Builder<>(changeTypeProvider);
    }

    /** Returns the provider that classifies each non-skipped record. */
    public CdcChangeTypeProvider<? super T> getChangeTypeProvider() {
        return changeTypeProvider;
    }

    /** Returns the sequence provider, or {@code null} when rows carry no sequence number. */
    @Nullable
    public CdcSequenceNumberProvider<? super T> getSequenceNumberProvider() {
        return sequenceNumberProvider;
    }

    /** Returns whether every non-skipped row must carry a sequence number. */
    public boolean hasSequenceNumberProvider() {
        return sequenceNumberProvider != null;
    }

    /** Builder for {@link CdcOptions}. */
    @Public
    public static final class Builder<T> {

        private final CdcChangeTypeProvider<? super T> changeTypeProvider;
        @Nullable private CdcSequenceNumberProvider<? super T> sequenceNumberProvider;

        private Builder(CdcChangeTypeProvider<? super T> changeTypeProvider) {
            this.changeTypeProvider =
                    Preconditions.checkNotNull(
                            changeTypeProvider, "changeTypeProvider must not be null");
        }

        /**
         * Sets the sequence provider. Once configured, every non-skipped record must produce a
         * valid sequence.
         */
        public Builder<T> sequenceNumberProvider(
                CdcSequenceNumberProvider<? super T> sequenceNumberProvider) {
            this.sequenceNumberProvider =
                    Preconditions.checkNotNull(
                            sequenceNumberProvider, "sequenceNumberProvider must not be null");
            return this;
        }

        /** Builds the immutable options. */
        public CdcOptions<T> build() {
            return new CdcOptions<>(this);
        }
    }
}
