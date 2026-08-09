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

package io.github.flink.gcp.connector.bigtable.source.readrows.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.TableId;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.BigtableDataClients;
import io.github.flink.gcp.connector.bigtable.TableDestination;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Samples row keys through a {@code google-cloud-bigtable} {@link BigtableDataClient}.
 *
 * <p>Named after the client its {@link #close()} releases, which is the connector's convention for
 * the real implementation of a seam.
 *
 * <p>The client is built on first use and closed once, both on the coordinator's side of the job:
 * the enumerator samples a table once and then has no further use for it. That is also why there is
 * no client cache here — the sink caches per instance because it writes continuously to many
 * tables, while this makes one call to one table.
 *
 * <p>The application profile is carried, and has to be: a Data Boost profile applies to {@code
 * SampleRowKeys} as well as to {@code ReadRows}, so a sampler that ignored it would plan a scan on
 * one kind of compute and then run it on another.
 *
 * <p>The client's retry configuration is left alone. It retries {@code SampleRowKeys} on the
 * transient codes under a total timeout of its own, so a failure that reaches the enumerator has
 * already exhausted the retry the client owns.
 */
@Internal
public final class DataClientRowKeySampler implements RowKeySampler {

    private static final long serialVersionUID = 1L;

    @Nullable private final String appProfileId;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    /**
     * The client, built on first use.
     *
     * <p>Transient because this sampler is serialized into the job graph; {@code volatile} and
     * guarded by the monitor below because {@link #sample} runs on the executor {@code
     * SplitEnumeratorContext#callAsync} hands the work to, while {@link #close()} runs on the
     * coordinator thread.
     */
    @Nullable private transient volatile BigtableDataClient client;

    private transient volatile boolean closed;

    /**
     * Creates the sampler.
     *
     * @param appProfileId the application profile to route through, or {@code null} for the
     *     instance's default
     * @param emulatorEndpoint the emulator endpoint (plaintext, no credentials), or {@code null}
     *     for production Bigtable
     */
    public DataClientRowKeySampler(
            @Nullable String appProfileId, @Nullable EmulatorEndpoint emulatorEndpoint) {
        this.appProfileId = appProfileId;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    public List<RowKeySample> sample(TableDestination table) throws IOException {
        // The TargetId overload, not the String one: that one is deprecated. TableId is the
        // TargetId a table has; authorized views are the other one and are out of scope here.
        List<KeyOffset> keyOffsets = client(table).sampleRowKeys(TableId.of(table.getTable()));
        List<RowKeySample> samples = new ArrayList<>(keyOffsets.size());
        for (KeyOffset keyOffset : keyOffsets) {
            samples.add(RowKeySample.of(keyOffset.getKey(), keyOffset.getOffsetBytes()));
        }
        return samples;
    }

    @Override
    public void close() throws IOException {
        BigtableDataClient toClose;
        synchronized (this) {
            closed = true;
            toClose = client;
            client = null;
        }
        if (toClose != null) {
            toClose.close();
        }
    }

    /**
     * Returns the client, building it on first use.
     *
     * <p>The monitor is this object rather than a lock field, because a lock field would have to
     * travel in the job graph.
     */
    private BigtableDataClient client(TableDestination table) throws IOException {
        BigtableDataClient existing = client;
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            if (closed) {
                throw new IOException(
                        "The Bigtable row key sampler for "
                                + table
                                + " was closed before it was used.");
            }
            if (client == null) {
                client = BigtableDataClient.create(settings(table));
            }
            return client;
        }
    }

    /**
     * Builds the client settings. Visible to the module's tests because the mapping is otherwise
     * observable only through the client's behaviour: an application profile that never reaches the
     * client looks exactly like one that does.
     */
    @VisibleForTesting
    BigtableDataSettings settings(TableDestination table) {
        return BigtableDataClients.settings(table, appProfileId, emulatorEndpoint).build();
    }
}
