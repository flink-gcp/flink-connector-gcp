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
import com.google.cloud.storage.StorageOptions;
import io.github.flink.gcp.connector.bigquery.BigQueryCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.List;

/**
 * {@link StagingStorage} over the Cloud Storage client with configured credentials or ADC.
 *
 * <p>GCS resumable uploads buffer one chunk in memory per open object; the chunk size is lowered
 * from the client default (16 MiB) to bound the writer's footprint, which grows with the number of
 * concurrently open destinations.
 *
 * <p>The client is created lazily so instances can be constructed on the client side, serialized
 * into the job graph, and only connect once running.
 */
@Internal
public final class GcsStagingStorage implements StagingStorage {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(GcsStagingStorage.class);

    private static final int UPLOAD_CHUNK_BYTES = 4 * 1024 * 1024;

    /** Objects per batched delete request (the GCS batch API accepts up to 100 operations). */
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

    /** Returns the configured key-file path, or {@code null} for ADC. */
    @VisibleForTesting
    @Nullable
    public String getServiceAccountKeyFile() {
        return serviceAccountKeyFile;
    }

    @Override
    public OutputStream createObject(String gcsUri) throws IOException {
        BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.fromGsUtilUri(gcsUri)).build();
        WriteChannel channel = storage().writer(blobInfo);
        channel.setChunkSize(UPLOAD_CHUNK_BYTES);
        return Channels.newOutputStream(channel);
    }

    @Override
    public void deleteObjects(List<String> gcsUris) {
        int failed = 0;
        for (int from = 0; from < gcsUris.size(); from += DELETE_BATCH_SIZE) {
            List<String> chunk =
                    gcsUris.subList(from, Math.min(from + DELETE_BATCH_SIZE, gcsUris.size()));
            BlobId[] blobIds = new BlobId[chunk.size()];
            for (int i = 0; i < chunk.size(); i++) {
                blobIds[i] = BlobId.fromGsUtilUri(chunk.get(i));
            }
            try {
                for (Boolean deleted : storage().delete(blobIds)) {
                    if (!Boolean.TRUE.equals(deleted)) {
                        failed++;
                    }
                }
            } catch (IOException | RuntimeException e) {
                failed += chunk.size();
                LOG.warn("Failed to delete a batch of {} staging objects", chunk.size(), e);
            }
        }
        if (failed > 0) {
            LOG.warn(
                    "Failed to delete {} of {} staging objects; a bucket lifecycle rule is"
                            + " recommended to expire leftovers",
                    failed,
                    gcsUris.size());
        }
    }

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
