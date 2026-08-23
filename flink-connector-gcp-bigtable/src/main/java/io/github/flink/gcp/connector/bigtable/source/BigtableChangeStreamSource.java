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

package io.github.flink.gcp.connector.bigtable.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.google.api.gax.core.CredentialsProvider;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.source.ReaderInitializationContext;
import io.github.flink.gcp.connector.bigtable.BigtableCredentials;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamEnumeratorState;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamEnumeratorStateSerializer;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplitSerializer;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.BigtableChangeStreamSplitEnumerator;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.ChangeStreamCoordinatorClient;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.BigtableChangeStreamReader;

import javax.annotation.Nullable;

/** FLIP-27 source for Bigtable Change Streams. */
@PublicEvolving
public final class BigtableChangeStreamSource<T>
        implements Source<T, ChangeStreamPartitionSplit, BigtableChangeStreamEnumeratorState>,
                ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;
    private final BigtableChangeStreamSourceConfig<T> config;

    BigtableChangeStreamSource(BigtableChangeStreamSourceConfig<T> config) {
        this.config = config;
    }

    /**
     * Returns a builder for a Bigtable Change Streams source.
     *
     * @param <T> the record type produced
     * @return the builder
     */
    public static <T> BigtableChangeStreamSourceBuilder<T> builder() {
        return new BigtableChangeStreamSourceBuilder<>();
    }

    /** Returns the source configuration for tests. */
    @VisibleForTesting
    BigtableChangeStreamSourceConfig<T> getConfig() {
        return config;
    }

    @Override
    public Boundedness getBoundedness() {
        return config.getBoundedTimestamp() == null
                ? Boundedness.CONTINUOUS_UNBOUNDED
                : Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<T, ChangeStreamPartitionSplit> createReader(SourceReaderContext context)
            throws Exception {
        // One provider for both of the reader's seams: the opener streams changes through a data
        // client while the restore resolver reads retention through a table-admin one, so a load
        // per seam would scope each for half of what this reader does.
        CredentialsProvider credentials =
                BigtableCredentials.loadDataAndTableAdmin(config.getServiceAccountKeyFile());
        config.getOpener().useCredentials(credentials);
        config.getRestoreResolver().useCredentials(credentials);
        config.getDeserializer().open(new ReaderInitializationContext(context));
        return new BigtableChangeStreamReader<>(context, config);
    }

    @Override
    public SplitEnumerator<ChangeStreamPartitionSplit, BigtableChangeStreamEnumeratorState>
            createEnumerator(SplitEnumeratorContext<ChangeStreamPartitionSplit> context)
                    throws Exception {
        return enumerator(context, null);
    }

    @Override
    public SplitEnumerator<ChangeStreamPartitionSplit, BigtableChangeStreamEnumeratorState>
            restoreEnumerator(
                    SplitEnumeratorContext<ChangeStreamPartitionSplit> context,
                    BigtableChangeStreamEnumeratorState checkpoint)
                    throws Exception {
        return enumerator(context, checkpoint);
    }

    /**
     * Builds one enumerator and the one coordinator client it owns.
     *
     * @param context the enumerator context
     * @param restored the checkpointed ledger, or {@code null} for a fresh enumerator
     * @return the enumerator
     * @throws Exception if the client cannot be created
     */
    private BigtableChangeStreamSplitEnumerator enumerator(
            SplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            @Nullable BigtableChangeStreamEnumeratorState restored)
            throws Exception {
        // A client per enumerator, minted here rather than carried on the configuration: this
        // object is what the JobManager keeps for a job's whole life, and a coordinator reset
        // builds the next enumerator from it (docs/adr/0128). The factory loads one provider for
        // the coordinator's three client families: data for partition discovery, table admin for
        // retention, instance admin for the app profile.
        ChangeStreamCoordinatorClient client = config.getCoordinatorClientFactory().create();
        try {
            return new BigtableChangeStreamSplitEnumerator(
                    context,
                    client,
                    config.getStartPosition(),
                    config.getResumeFallback(),
                    restored,
                    config.getBoundedTimestamp() != null);
        } catch (Throwable e) {
            // The enumerator never took ownership, so nothing else will close what was just minted.
            Closers.closeAllSuppressing(e, client);
            throw e;
        }
    }

    @Override
    public SimpleVersionedSerializer<ChangeStreamPartitionSplit> getSplitSerializer() {
        return new ChangeStreamPartitionSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<BigtableChangeStreamEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        return new BigtableChangeStreamEnumeratorStateSerializer();
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return config.getDeserializer().getProducedType();
    }
}
