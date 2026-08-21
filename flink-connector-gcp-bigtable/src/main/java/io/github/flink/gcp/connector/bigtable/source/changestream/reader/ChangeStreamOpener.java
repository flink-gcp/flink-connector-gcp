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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import org.apache.flink.annotation.Internal;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.ResponseObserver;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;

/**
 * Serializable seam that opens asynchronous {@code ReadChangeStream} RPCs.
 *
 * <p>An implementation carries no service-account key-file path and loads no credentials of its
 * own. The reader that owns it loads one provider for every client family it owns and hands that
 * provider over through {@link #useCredentials(CredentialsProvider)}, which is what lets the stream
 * client and the restore resolver's admin client share one.
 */
@Internal
public interface ChangeStreamOpener extends Serializable, AutoCloseable {

    void open(
            TableDestination table,
            ChangeStreamPartitionSplit split,
            @Nullable Instant endTime,
            ResponseObserver<ChangeStreamRecord> observer)
            throws IOException;

    /**
     * Receives the provider the owning reader loaded, before the first {@link #open}.
     *
     * <p>Declared here rather than on the implementation so that the reader needs no cast, and
     * abstract rather than defaulted because an implementation that quietly skipped it would open
     * its stream as the process's application default credentials instead of the configured service
     * account — a misconfiguration nothing would report.
     *
     * @param credentials the provider to build clients with, or {@code null} to leave the client's
     *     application default credentials in place
     */
    void useCredentials(@Nullable CredentialsProvider credentials);

    @Override
    void close() throws IOException;
}
