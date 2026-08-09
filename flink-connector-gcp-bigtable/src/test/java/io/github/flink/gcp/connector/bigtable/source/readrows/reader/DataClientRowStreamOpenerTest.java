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

package io.github.flink.gcp.connector.bigtable.source.readrows.reader;

import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.TestSources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the settings {@link DataClientRowStreamOpener} builds, and for its lifecycle.
 *
 * <p>The application profile has its own test here rather than only on {@link
 * io.github.flink.gcp.connector.bigtable.BigtableDataClients}: an opener that simply never passed
 * the profile on would leave that shared test green while reading through the wrong compute.
 */
@Timeout(30)
class DataClientRowStreamOpenerTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");

    @Test
    void carriesTheApplicationProfileAndTheEmulatorEndpointToTheClient() {
        BigtableDataSettings settings =
                new DataClientRowStreamOpener(
                                "boost-profile", EmulatorEndpoint.parse("bigtable.example:9035"))
                        .settings(TABLE);

        assertThat(settings.getProjectId()).isEqualTo("p");
        assertThat(settings.getInstanceId()).isEqualTo("i");
        assertThat(settings.getAppProfileId()).isEqualTo("boost-profile");
        assertThat(settings.getStubSettings().getEndpoint()).isEqualTo("bigtable.example:9035");
    }

    @Test
    void travelsInTheJobGraph() throws Exception {
        DataClientRowStreamOpener opener =
                new DataClientRowStreamOpener(
                        "boost-profile", EmulatorEndpoint.parse("bigtable.example:9035"));

        DataClientRowStreamOpener back =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(opener), getClass().getClassLoader());

        assertThat(back.settings(TABLE).getAppProfileId()).isEqualTo("boost-profile");
    }

    @Test
    void theBuilderWiresTheApplicationProfileIntoThisSeam() {
        // The reader's half of the same gap: both seams build their own client, so a profile that
        // reached only one of them would plan the scan on one kind of compute and run it on
        // another.
        DataClientRowStreamOpener opener =
                (DataClientRowStreamOpener)
                        TestSources.config(builder -> builder.appProfileId("boost-profile"))
                                .getOpener();

        assertThat(opener.settings(TABLE).getAppProfileId()).isEqualTo("boost-profile");
    }

    @Test
    void closingWithoutHavingOpenedReleasesNothingAndFailsNothing() {
        assertThatCode(() -> new DataClientRowStreamOpener(null, null).close())
                .doesNotThrowAnyException();
    }

    @Test
    void refusesToBuildAClientAfterItHasBeenClosed() throws IOException {
        DataClientRowStreamOpener opener =
                new DataClientRowStreamOpener(null, EmulatorEndpoint.parse("localhost:1"));
        opener.close();

        assertThatThrownBy(() -> opener.open(TABLE, ByteStringRange.unbounded(), null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("was closed before it was used");
    }
}
