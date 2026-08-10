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

package io.github.flink.gcp.connector.bigquery;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

/**
 * Cloud Storage plumbing for the gated FILE_LOADS ITCases: the {@code BQ_IT_GCS_BUCKET} variable, a
 * client over the same application-default credentials {@link RealBigQuery} uses, and
 * staging-prefix upload, listing and cleanup.
 *
 * <p>A sibling of {@link RealBigQuery} rather than part of it: only FILE_LOADS stages anything, so
 * the BigQuery helper knows nothing about a bucket. It sits beside it all the same, because both
 * are plumbing over a gate variable and the three gates are worth finding in one place.
 *
 * <p>Named for the product and not {@code RealStorage}: {@code sink.storage} in this module means
 * the Storage <em>Write</em> API, which this is not.
 */
public final class RealGcs {

    private RealGcs() {}

    /**
     * The bucket the gated FILE_LOADS ITCases stage into ({@code BQ_IT_GCS_BUCKET}). Private, as
     * {@link #client()} is: a caller wants a URI or the objects under a prefix, not the bucket
     * name, and keeping it that way is what stops a second {@code gs://} concatenation appearing.
     */
    private static String bucket() {
        return System.getenv("BQ_IT_GCS_BUCKET");
    }

    /** A client over application-default credentials, in {@link RealBigQuery#project()}. */
    private static Storage client() {
        return StorageOptions.newBuilder()
                .setProjectId(RealBigQuery.project())
                .build()
                .getService();
    }

    /**
     * The {@code gs://} URI of {@code path} in the staging bucket, for {@code stagingPath(...)}.
     */
    public static String uri(String path) {
        return "gs://" + bucket() + "/" + path;
    }

    /** Uploads {@code content} at {@code path} in the staging bucket. */
    public static void upload(String path, byte[] content) {
        client().create(BlobInfo.newBuilder(bucket(), path).build(), content);
    }

    /** The objects currently under {@code prefix}. */
    public static Iterable<Blob> list(String prefix) {
        return client().list(bucket(), Storage.BlobListOption.prefix(prefix)).iterateAll();
    }

    /** Deletes everything under {@code prefix}; a successful load leaves nothing behind. */
    public static void deletePrefix(String prefix) {
        for (Blob blob : list(prefix)) {
            blob.delete();
        }
    }
}
