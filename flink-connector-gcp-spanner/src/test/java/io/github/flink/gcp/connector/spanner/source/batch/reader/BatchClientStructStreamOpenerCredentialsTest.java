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

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.CustomOpenTelemetryMetricsProvider;
import com.google.cloud.spanner.SpannerOptions;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.spanner.ClientMetricsProbe;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.SpannerCredentials;
import io.github.flink.gcp.connector.testutils.ServiceAccountKeyFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

class BatchClientStructStreamOpenerCredentialsTest {
    @TempDir Path tempDir;

    @Test
    void carriesTheCredentialsTheReaderHandedItIntoTheClientSettings() throws Exception {
        GoogleCredentials loaded =
                SpannerCredentials.load(ServiceAccountKeyFiles.create(tempDir).toString());
        BatchClientStructStreamOpener opener =
                new BatchClientStructStreamOpener(DatabaseDestination.of("p", "i", "d"), null);

        opener.useCredentials(loaded);

        assertThat(opener.settings().getCredentials()).isSameAs(loaded);
    }

    @Test
    void carriesTheClientMetricsChoicesIntoTheClientSettings() throws Exception {
        ClientMetrics metrics = ClientMetricsProbe.create();
        BatchClientStructStreamOpener opener =
                new BatchClientStructStreamOpener(
                        DatabaseDestination.of("p", "i", "d"), null, false);
        opener.useCredentials(
                GoogleCredentials.create(new AccessToken("token", new Date(Long.MAX_VALUE))));

        opener.useClientMetrics(metrics);

        SpannerOptions settings = opener.settings();
        assertThat(settings.getClientMetricsProvider())
                .isInstanceOfSatisfying(
                        CustomOpenTelemetryMetricsProvider.class,
                        provider ->
                                assertThat(provider.getOpenTelemetry())
                                        .isSameAs(metrics.openTelemetry()));
        assertThat(settings.isEnableBuiltInMetrics()).isFalse();
    }

    @Test
    void closesTheClientMetricsItWasHandedEvenWhenItNeverRead() throws Exception {
        ClientMetrics metrics = ClientMetricsProbe.create();
        BatchClientStructStreamOpener opener =
                new BatchClientStructStreamOpener(DatabaseDestination.of("p", "i", "d"), null);
        opener.useClientMetrics(metrics);

        opener.close();

        assertThat(ClientMetricsProbe.isOpen(metrics)).isFalse();
    }
}
