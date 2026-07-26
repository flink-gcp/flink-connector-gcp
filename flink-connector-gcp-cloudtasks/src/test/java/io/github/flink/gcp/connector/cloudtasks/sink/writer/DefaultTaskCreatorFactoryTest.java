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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DefaultTaskCreatorFactory}. */
class DefaultTaskCreatorFactoryTest {

    @Test
    void buildsAndClosesAnEmulatorBackedCreatorWithoutCredentials() throws Exception {
        // Nothing here talks to the endpoint: the channel connects lazily, so this covers the
        // plaintext/no-credentials wiring the emulator integration tests (#25) build on.
        TaskCreator creator = new DefaultTaskCreatorFactory("localhost:8123").create();

        assertThat(creator).isNotNull();
        creator.close();
    }

    @Test
    void isSerializableIntoTheJobGraph() throws Exception {
        DefaultTaskCreatorFactory factory = new DefaultTaskCreatorFactory("localhost:8123");

        Object restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(factory), getClass().getClassLoader());

        assertThat(restored).isInstanceOf(DefaultTaskCreatorFactory.class);
    }
}
