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

package io.github.flink.gcp.connector.cloudtasks.sink.writer;

import org.apache.flink.util.InstantiationUtil;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.tasks.v2.CloudTasksSettings;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DefaultTaskCreatorFactory}. */
class DefaultTaskCreatorFactoryTest {

    @TempDir Path tempDir;

    @Test
    void configuredCredentialsReachTheClientSettings() throws Exception {
        Path keyFile = ServiceAccountKeyFileTestUtil.write(tempDir);

        CloudTasksSettings settings =
                DefaultTaskCreatorFactory.productionSettings(keyFile.toString()).build();

        assertThat(settings.getCredentialsProvider()).isInstanceOf(FixedCredentialsProvider.class);
        assertThat(settings.getCredentialsProvider().getCredentials())
                .isInstanceOf(ServiceAccountCredentials.class);
    }

    @Test
    void absentConfiguredCredentialsLeaveApplicationDefaultsInEffect() throws Exception {
        CloudTasksSettings settings = DefaultTaskCreatorFactory.productionSettings(null).build();

        assertThat(settings.getCredentialsProvider()).isInstanceOf(GoogleCredentialsProvider.class);
    }

    @Test
    void buildsAndClosesAProductionCreatorWithConfiguredCredentials() throws Exception {
        Path keyFile = ServiceAccountKeyFileTestUtil.write(tempDir);

        TaskCreator creator = new DefaultTaskCreatorFactory(keyFile.toString(), null).create();

        assertThat(creator).isNotNull();
        creator.close();
    }

    @Test
    void buildsAndClosesAnEmulatorBackedCreatorWithoutCredentials() throws Exception {
        // Nothing here talks to the endpoint: the channel connects lazily, so this covers the
        // plaintext/no-credentials wiring the emulator integration tests (#25) build on.
        TaskCreator creator =
                new DefaultTaskCreatorFactory(EmulatorEndpoint.parse("localhost:8123")).create();

        assertThat(creator).isNotNull();
        creator.close();
    }

    @Test
    void isSerializableIntoTheJobGraph() throws Exception {
        DefaultTaskCreatorFactory factory =
                new DefaultTaskCreatorFactory(EmulatorEndpoint.parse("localhost:8123"));

        Object restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(factory), getClass().getClassLoader());

        assertThat(restored).isInstanceOf(DefaultTaskCreatorFactory.class);
    }
}
