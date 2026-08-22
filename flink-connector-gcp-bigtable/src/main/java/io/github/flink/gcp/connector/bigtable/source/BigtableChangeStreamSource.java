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

import org.apache.flink.annotation.Public;
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
import io.github.flink.gcp.connector.base.source.ReaderInitializationContext;
import io.github.flink.gcp.connector.bigtable.BigtableCredentials;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamEnumeratorState;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamEnumeratorStateSerializer;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplitSerializer;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.BigtableChangeStreamSplitEnumerator;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.ChangeStreamCoordinatorClient;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.DefaultChangeStreamCoordinatorClient;
import io.github.flink.gcp.connector.bigtable.source.changestream.reader.BigtableChangeStreamReader;

/** FLIP-27 source for Bigtable Change Streams. */
@Public
public final class BigtableChangeStreamSource<T>
        implements Source<T, ChangeStreamPartitionSplit, BigtableChangeStreamEnumeratorState>,
                ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;
    private final BigtableChangeStreamSourceConfig<T> config;

    BigtableChangeStreamSource(BigtableChangeStreamSourceConfig<T> config) {
        this.config = config;
    }

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
        return config.getEndTime() == null ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED;
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

    private BigtableChangeStreamSplitEnumerator enumerator(
            SplitEnumeratorContext<ChangeStreamPartitionSplit> context,
            BigtableChangeStreamEnumeratorState restored)
            throws Exception {
        ChangeStreamCoordinatorClient client = config.getCoordinatorClient();
        if (client == null) {
            // One provider for the coordinator's three client families: data for partition
            // discovery, table admin for retention, instance admin for the app profile.
            client =
                    new DefaultChangeStreamCoordinatorClient(
                            config.getTable(),
                            config.getAppProfileId(),
                            BigtableCredentials.loadAll(config.getServiceAccountKeyFile()));
        }
        return new BigtableChangeStreamSplitEnumerator(
                context,
                client,
                config.getStartPosition(),
                config.getResumeFallback(),
                restored,
                config.getEndTime() != null,
                true);
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
