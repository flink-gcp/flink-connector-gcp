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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageBatchResult;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import io.github.flink.gcp.connector.bigquery.BigQueryCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link StagingStorage} over the Cloud Storage client with configured credentials or ADC.
 *
 * <p>GCS resumable uploads buffer one chunk in memory per open object. The writer passes {@code
 * FileLoadsOptions#getStagingUploadChunkBytes()}, whose 4 MiB default is lower than the client's 16
 * MiB to bound the writer's footprint, which grows with the number of concurrently open
 * destinations.
 *
 * <p>The client is created lazily so instances can be constructed on the client side, serialized
 * into the job graph, and only connect once running.
 */
@Internal
public final class GcsStagingStorage implements StagingStorage {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(GcsStagingStorage.class);

    /**
     * Objects per submitted batch. The client already splits a batch into requests of 100, the most
     * the GCS batch API accepts, but it stops at the first request that fails as a whole;
     * submitting each 100 separately still attempts the rest.
     */
    private static final int DELETE_BATCH_SIZE = 100;

    private transient Storage storage;
    @Nullable private final String serviceAccountKeyFile;

    /** Creates staging storage using application-default credentials. */
    public GcsStagingStorage() {
        this(null);
    }

    /** Creates staging storage with optional runtime-loaded service-account credentials. */
    public GcsStagingStorage(@Nullable String serviceAccountKeyFile) {
        this.serviceAccountKeyFile = serviceAccountKeyFile;
    }

    /** Creates staging storage over an already built client, for tests. */
    @VisibleForTesting
    static GcsStagingStorage withClient(Storage storage) {
        GcsStagingStorage staging = new GcsStagingStorage();
        staging.storage = storage;
        return staging;
    }

    /** Returns the configured key-file path, or {@code null} for ADC. */
    @VisibleForTesting
    @Nullable
    public String getServiceAccountKeyFile() {
        return serviceAccountKeyFile;
    }

    @Override
    public WritableByteChannel createObject(String gcsUri, int chunkBytes) throws IOException {
        BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.fromGsUtilUri(gcsUri)).build();
        WriteChannel channel = storage().writer(blobInfo);
        // The writer's staging uploader relies on this client's behaviour: it aligns its buffer to
        // 256 KiB, which FileLoadsOptions already requires, so the buffer is exactly one chunk, and
        // a full chunk written while the buffer is empty is sent without being copied. The buffer
        // is
        // allocated only by a write that leaves a remainder, such as a file's last, partial chunk.
        channel.setChunkSize(chunkBytes);
        return channel;
    }

    /**
     * {@inheritDoc}
     *
     * <p>The batch is built here rather than through {@code Storage.delete(BlobId...)}, which
     * reports a missing object and a refused deletion alike as {@code false}.
     */
    @Override
    public void deleteObjects(List<String> gcsUris) {
        int failed = 0;
        int notFound = 0;
        StorageException firstFailure = null;
        for (int from = 0; from < gcsUris.size(); from += DELETE_BATCH_SIZE) {
            List<String> chunk =
                    gcsUris.subList(from, Math.min(from + DELETE_BATCH_SIZE, gcsUris.size()));
            List<StorageBatchResult<Boolean>> results = new ArrayList<>(chunk.size());
            try {
                StorageBatch batch = storage().batch();
                for (String gcsUri : chunk) {
                    results.add(batch.delete(BlobId.fromGsUtilUri(gcsUri)));
                }
                batch.submit();
            } catch (IOException | RuntimeException e) {
                // Not a count of failed deletions: the results read below say which objects were
                // deleted or not found before the request failed, such as in a truncated response.
                LOG.warn(
                        "Could not complete a batched delete of {} staging objects",
                        chunk.size(),
                        e);
            }
            // An object with no result, or whose result never arrived because its request failed
            // as a whole, counts as a failure: nothing says it was deleted.
            failed += chunk.size() - results.size();
            for (StorageBatchResult<Boolean> result : results) {
                if (!result.completed()) {
                    failed++;
                    continue;
                }
                try {
                    if (!Boolean.TRUE.equals(result.get())) {
                        notFound++;
                    }
                } catch (StorageException e) {
                    failed++;
                    if (firstFailure == null) {
                        firstFailure = e;
                    }
                }
            }
        }
        if (notFound > 0) {
            LOG.debug("{} of {} staging objects were already deleted", notFound, gcsUris.size());
        }
        if (failed > 0) {
            // The first per-object failure, if any, names the cause, such as a missing
            // storage.objects.delete permission; a failed request was logged with its own above.
            LOG.warn(
                    "Failed to delete {} of {} staging objects; a bucket lifecycle rule is"
                            + " recommended to expire leftovers",
                    failed,
                    gcsUris.size(),
                    firstFailure);
        }
    }

    /**
     * Closes the client, if this instance ever built one. A client is built on the first {@link
     * #createObject} or {@link #deleteObjects}, so an instance that only ever crossed the job graph
     * has nothing to release.
     *
     * <p>On the HTTP transport this instance builds, the SDK's {@code close()} is the interface's
     * inherited no-op — {@code StorageImpl} does not override it. The call is here for the
     * contract, not for a leak that has been observed.
     */
    @Override
    public void close() throws Exception {
        if (storage != null) {
            storage.close();
        }
    }

    /**
     * Builds the client on the HTTP transport, the {@code StorageOptions} default. Two things rely
     * on that transport. {@link #deleteObjects(List)} calls {@code Storage.batch()}, which the gRPC
     * client rejects as HTTP-only. {@code StagedFileWriter.abort()} leaves the upload channel
     * unclosed, which on HTTP holds only a heap buffer; what an abandoned gRPC write holds has not
     * been checked. Revisit both before switching the transport.
     */
    private Storage storage() throws IOException {
        if (storage == null) {
            storage =
                    serviceAccountKeyFile == null
                            ? StorageOptions.getDefaultInstance().getService()
                            : productionOptions(serviceAccountKeyFile).getService();
        }
        return storage;
    }

    /** Builds production options carrying the configured service-account credentials. */
    @VisibleForTesting
    static StorageOptions productionOptions(String serviceAccountKeyFile) throws IOException {
        return StorageOptions.newBuilder()
                .setCredentials(BigQueryCredentials.load(serviceAccountKeyFile))
                .build();
    }
}
