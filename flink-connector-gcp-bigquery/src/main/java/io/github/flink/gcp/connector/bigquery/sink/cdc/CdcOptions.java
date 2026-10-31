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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Options for appending BigQuery CDC mutations through the Storage Write API default stream.
 *
 * <p>Both providers are required. The sequence provider must return a valid non-null sequence for
 * every record the configured serializer does not skip, and a key's sequence should grow with each
 * of its changes: BigQuery applies a key's changes by their sequence and falls back to ingestion
 * time for equal ones, which only {@code OrderingMode.PER_TABLE} keeps in arrival order (ADR-0193).
 * Instances are immutable and serializable.
 */
@Experimental
public final class CdcOptions<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final CdcChangeTypeProvider<? super T> changeTypeProvider;
    private final CdcSequenceNumberProvider<? super T> sequenceNumberProvider;

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

    /** Returns the provider that supplies each non-skipped record's sequence number. */
    public CdcSequenceNumberProvider<? super T> getSequenceNumberProvider() {
        return sequenceNumberProvider;
    }

    /** Builder for {@link CdcOptions}. */
    @Experimental
    public static final class Builder<T> {

        private final CdcChangeTypeProvider<? super T> changeTypeProvider;
        @Nullable private CdcSequenceNumberProvider<? super T> sequenceNumberProvider;

        private Builder(CdcChangeTypeProvider<? super T> changeTypeProvider) {
            this.changeTypeProvider =
                    Preconditions.checkNotNull(
                            changeTypeProvider, "changeTypeProvider must not be null");
        }

        /**
         * Sets the sequence provider, which {@link #build()} requires. Every non-skipped record
         * must produce a valid sequence, which should grow with each change of its primary key.
         */
        public Builder<T> sequenceNumberProvider(
                CdcSequenceNumberProvider<? super T> sequenceNumberProvider) {
            this.sequenceNumberProvider =
                    Preconditions.checkNotNull(
                            sequenceNumberProvider, "sequenceNumberProvider must not be null");
            return this;
        }

        /**
         * Builds the immutable options.
         *
         * @return the options
         * @throws IllegalStateException if no sequence provider was set
         */
        public CdcOptions<T> build() {
            Preconditions.checkState(
                    sequenceNumberProvider != null,
                    "CdcOptions requires a sequenceNumberProvider(...): without a"
                            + " _CHANGE_SEQUENCE_NUMBER, BigQuery orders one primary key's"
                            + " changes by ingestion time, which the default ordering mode does"
                            + " not keep in arrival order. A source without a change-log"
                            + " position can supply a value taken where each change is made"
                            + " that never repeats for one key and grows with each of its"
                            + " changes, such as the source's commit time in nanoseconds or an"
                            + " increasing ID, never a clock read in the pipeline, written as"
                            + " one to four '/'-separated hexadecimal sections of at most 16"
                            + " digits.");
            return new CdcOptions<>(this);
        }
    }
}
