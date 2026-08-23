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

package io.github.flink.gcp.connector.spanner.source.batch.enumerator;

import com.google.auth.oauth2.GoogleCredentials;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.SpannerCredentials;
import io.github.flink.gcp.connector.testutils.ServiceAccountKeyFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class BatchClientPartitionPlannerCredentialsTest {
    @TempDir Path tempDir;

    @Test
    void carriesTheCredentialsTheEnumeratorHandedItIntoTheClientSettings() throws Exception {
        GoogleCredentials loaded =
                SpannerCredentials.load(ServiceAccountKeyFiles.create(tempDir).toString());
        BatchClientPartitionPlanner planner =
                new BatchClientPartitionPlanner(DatabaseDestination.of("p", "i", "d"), null);

        planner.useCredentials(loaded);

        assertThat(planner.settings().getCredentials()).isSameAs(loaded);
    }
}
