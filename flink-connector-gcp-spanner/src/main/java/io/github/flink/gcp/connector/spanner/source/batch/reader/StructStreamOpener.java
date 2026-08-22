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

package io.github.flink.gcp.connector.spanner.source.batch.reader;

import org.apache.flink.annotation.Internal;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.spanner.Partition;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/**
 * Rejoins a batch read's snapshot and opens one of its partitions.
 *
 * <p>The seam the readers read through. Serializable because it travels in the job graph inside the
 * source configuration; one per reader, closed when the reader closes.
 *
 * <p>It carries no service-account key-file path: the reader that owns it loads one set of
 * credentials and hands them over through {@link #useCredentials}, so an implementation neither
 * holds a path nor reads one.
 */
@Internal
public interface StructStreamOpener extends Serializable, AutoCloseable {

    /**
     * Opens a partition of a batch read.
     *
     * @param batchTransactionId the snapshot the partition belongs to
     * @param partition the partition to read
     * @return the open read
     * @throws IOException if the read cannot be opened
     */
    StructStream open(BatchTransactionId batchTransactionId, Partition partition)
            throws IOException;

    /**
     * Receives the credentials the owning reader loaded, before the first {@link
     * #open(BatchTransactionId, Partition)}.
     *
     * <p>Declared here rather than on the implementation so that the reader needs no cast, and
     * abstract rather than defaulted because an implementation that quietly skipped it would read
     * as the process's application default credentials instead of the configured service account —
     * a misconfiguration nothing would report.
     *
     * @param credentials what the reader loaded, or {@code null} to keep application default
     *     credentials
     */
    void useCredentials(@Nullable GoogleCredentials credentials);

    /**
     * Releases the client the reads were opened on.
     *
     * <p>It does <em>not</em> release the batch read's session: that is the enumerator's, and a
     * reader releasing it would end every other reader's read.
     *
     * @throws IOException if the release fails
     */
    @Override
    void close() throws IOException;
}
