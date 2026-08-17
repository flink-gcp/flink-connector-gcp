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

package io.github.flink.gcp.connector.spanner.sink.writer;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.Options;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.sink.SpannerWriterOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DefaultSpannerDatabaseAccessFactory}. */
class DefaultSpannerDatabaseAccessFactoryTest {

    private static final SpannerDatabase DATABASE =
            SpannerDatabase.of("my-project", "my-instance", "my-db");

    @Test
    void unsetKnobsAddNoTransactionOptionAtAll() {
        // Leaving the service's own handling in place, rather than restating today's defaults and
        // freezing them into every job.
        assertThat(
                        DefaultSpannerDatabaseAccessFactory.transactionOptions(
                                SpannerWriterOptions.defaults()))
                .isEmpty();
    }

    @Test
    void carriesTheCommitDelayAndPriorityWhenTheyAreSet() {
        Options.TransactionOption[] transactionOptions =
                DefaultSpannerDatabaseAccessFactory.transactionOptions(
                        SpannerWriterOptions.builder()
                                .maxCommitDelay(Duration.ofMillis(20))
                                .rpcPriority(SpannerRpcPriority.LOW)
                                .build());

        // Asserted by kind rather than by value, because the client library reads a transaction
        // option only from inside its own package: the accessors that would say what these carry
        // are package-private, as is the aggregate they fold into. Comparing against what the
        // library's own factories produce keeps the assertion off any private class name.
        assertThat(transactionOptions)
                .hasSize(2)
                .anyMatch(option -> option.getClass() == commitDelayKind())
                .anyMatch(option -> option.getClass() == priorityKind());
    }

    @Test
    void injectsWriterRuntimeCredentialsIntoClientSettings() {
        GoogleCredentials credentials =
                GoogleCredentials.create(new AccessToken("token", new Date(Long.MAX_VALUE)));
        DefaultSpannerDatabaseAccessFactory factory =
                new DefaultSpannerDatabaseAccessFactory(
                        DATABASE, SpannerWriterOptions.defaults(), null, credentials);

        assertThat(factory.settings().getCredentials() == credentials).isTrue();
    }

    @ParameterizedTest
    @EnumSource(SpannerRpcPriority.class)
    void mapsEveryPriorityThisConnectorOffers(SpannerRpcPriority priority) {
        // A value added to either enum has to fail somewhere; the mapping is a switch with no
        // default fall-through, so this pins that every one of ours reaches it.
        assertThat(
                        DefaultSpannerDatabaseAccessFactory.transactionOptions(
                                SpannerWriterOptions.builder().rpcPriority(priority).build()))
                .singleElement()
                .matches(option -> option.getClass() == priorityKind());
    }

    private static Class<?> commitDelayKind() {
        return Options.maxCommitDelay(Duration.ofMillis(1)).getClass();
    }

    private static Class<?> priorityKind() {
        return Options.priority(Options.RpcPriority.LOW).getClass();
    }

    @Test
    void releasesTheServiceHandleWhenTheDatabaseClientCannotBeOpened() {
        // The handle is the factory's until the adapter takes it over, and nothing downstream
        // would ever close it — a job restarting on a bad DatabaseId would otherwise leak one
        // channel pool per attempt.
        FakeSpanner spanner = new FakeSpanner(new IllegalStateException("no such database"));

        assertThatThrownBy(() -> factory("localhost:9010").create(spanner))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("no such database");

        assertThat(spanner.closes()).isEqualTo(1);
    }

    private static DefaultSpannerDatabaseAccessFactory factory(String emulatorEndpoint) {
        return new DefaultSpannerDatabaseAccessFactory(
                DATABASE,
                SpannerWriterOptions.defaults(),
                EmulatorEndpoint.parse(emulatorEndpoint, "emulatorEndpoint"));
    }
}
