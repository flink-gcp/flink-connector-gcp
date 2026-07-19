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

package io.github.flink.gcp.connector.bigquery.sink.fileloads;

import org.apache.flink.annotation.Internal;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.List;

/**
 * {@link StagingStorage} over the Cloud Storage client with application-default credentials.
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

    private transient Storage storage;

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
        for (String gcsUri : gcsUris) {
            try {
                storage().delete(BlobId.fromGsUtilUri(gcsUri));
            } catch (RuntimeException e) {
                failed++;
                LOG.warn("Failed to delete staging object {}", gcsUri, e);
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

    private Storage storage() {
        if (storage == null) {
            storage = StorageOptions.getDefaultInstance().getService();
        }
        return storage;
    }
}
