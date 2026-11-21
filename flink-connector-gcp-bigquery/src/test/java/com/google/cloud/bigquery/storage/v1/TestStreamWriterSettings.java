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

package com.google.cloud.bigquery.storage.v1;

import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;

import java.io.IOException;

/**
 * Reads the transport provider the SDK builds for a pooled default-stream writer, from inside the
 * package that owns it.
 *
 * <p><b>Why this class is in {@code com.google.cloud.bigquery.storage.v1}.</b> The connector
 * replaces that provider to put the writer's channels on its own event-loop group, which means
 * restating the SDK's settings ({@code docs/adr/0208}). The only place they are written down is the
 * package-private {@code StreamWriter.getBigQueryWriteSettings(Builder)}, so a test comparing the
 * restated provider against the SDK's has to call it from here — the reach {@code docs/adr/0067}
 * records for {@code com.google.cloud.bigquery.TestJobs}. A release that moves the method breaks
 * this file at compile time, in a test. Verified against google-cloud-bigquerystorage 3.38.0.
 */
public final class TestStreamWriterSettings {

    private TestStreamWriterSettings() {}

    /**
     * Returns the transport provider the SDK builds for a default-stream writer with no provider,
     * client or credentials of its own.
     *
     * @return the SDK's provider
     * @throws IOException if the SDK cannot build the settings
     */
    public static InstantiatingGrpcChannelProvider pooledWriterProvider() throws IOException {
        StreamWriter.Builder builder =
                StreamWriter.newBuilder("projects/p/datasets/d/tables/t/_default")
                        .setEnableConnectionPool(true);
        return (InstantiatingGrpcChannelProvider)
                StreamWriter.getBigQueryWriteSettings(builder).getTransportChannelProvider();
    }
}
