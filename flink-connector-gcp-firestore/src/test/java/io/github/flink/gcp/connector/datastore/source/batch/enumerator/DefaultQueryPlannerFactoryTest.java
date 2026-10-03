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

package io.github.flink.gcp.connector.datastore.source.batch.enumerator;

import org.apache.flink.util.InstantiationUtil;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The ADR-0128 tripwire: the factory travels in the job graph, the planner it mints does not. */
class DefaultQueryPlannerFactoryTest {

    @Test
    void theFactoryTravelsInTheJobGraphAndThePlannerDoesNot() throws Exception {
        DefaultQueryPlannerFactory factory =
                new DefaultQueryPlannerFactory(
                        EmulatorEndpoint.parse("localhost:1", "emulatorEndpoint"));

        DefaultQueryPlannerFactory back =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(factory), getClass().getClassLoader());

        try (QueryPlanner planner = back.create()) {
            assertThat(planner).isInstanceOf(ClientQueryPlanner.class);
        }
        assertThat(java.io.Serializable.class.isAssignableFrom(QueryPlanner.class))
                .as("a serializable planner could be parked on the configuration again")
                .isFalse();
    }
}
