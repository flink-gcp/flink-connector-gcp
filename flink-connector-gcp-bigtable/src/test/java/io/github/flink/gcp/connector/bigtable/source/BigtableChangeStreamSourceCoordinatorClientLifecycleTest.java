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

package io.github.flink.gcp.connector.bigtable.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamEnumeratorState;
import io.github.flink.gcp.connector.bigtable.source.changestream.BigtableChangeStreamMutation;
import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamPartitionSplit;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.ChangeStreamCoordinatorClient;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.ChangeStreamCoordinatorClientFactory;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.DefaultChangeStreamCoordinatorClient;
import io.github.flink.gcp.connector.bigtable.source.changestream.enumerator.DefaultChangeStreamCoordinatorClientFactory;
import io.github.flink.gcp.connector.bigtable.source.serializer.BigtableChangeStreamDeserializationSchema;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins that one coordinator client belongs to one enumerator.
 *
 * <p>This source already minted per enumerator on the branch where no client was configured, so it
 * never had the restart loop {@code docs/adr/0128} was written for. What it did have is the shape
 * that rule forbids: a configuration able to hold a live seam, reachable from the builder's
 * test-only setter. A double handed to two enumerators would have been closed by the first, and
 * {@code DefaultChangeStreamCoordinatorClient.close()} nulls its credentials as well as its clients
 * — so the second would have rebuilt as application default credentials with nothing reporting the
 * substitution.
 */
class BigtableChangeStreamSourceCoordinatorClientLifecycleTest {

    private static BigtableChangeStreamSource<String> source(RecordingFactory clients) {
        return BigtableChangeStreamSource.<String>builder()
                .table(TableDestination.of("project", "instance", "table"))
                .appProfileId("single-cluster")
                .deserializer(new TokenDeserializer())
                .startPosition(StartPosition.latest())
                .coordinatorClientFactory(clients)
                .build();
    }

    @Test
    void eachEnumeratorGetsItsOwnCoordinatorClient() throws Exception {
        RecordingFactory clients = new RecordingFactory();
        BigtableChangeStreamSource<String> source = source(clients);

        source.createEnumerator(new FakeSplitEnumeratorContext<>(1)).close();
        SplitEnumerator<ChangeStreamPartitionSplit, BigtableChangeStreamEnumeratorState> second =
                source.createEnumerator(new FakeSplitEnumeratorContext<>(1));

        assertThat(clients.minted).hasSize(2);
        assertThat(clients.minted.get(0)).isNotSameAs(clients.minted.get(1));
        assertThat(clients.minted.get(0).closes)
                .as("the first enumerator's client is the one its teardown ended")
                .isOne();
        assertThat(clients.minted.get(1).closes)
                .as("the second enumerator's client is untouched by that teardown")
                .isZero();

        second.close();
    }

    /**
     * Pins the window minting opens: between {@code create()} and the enumerator taking ownership,
     * the source is the only thing that can release the client. This source had no such release
     * before {@code docs/adr/0128}.
     */
    @Test
    void theSourceClosesAClientItCouldNotHandOver() {
        RecordingFactory clients = new RecordingFactory();
        BigtableChangeStreamSource<String> source = source(clients);

        assertThatThrownBy(() -> source.createEnumerator(null))
                .isInstanceOf(NullPointerException.class);

        assertThat(clients.minted).hasSize(1);
        assertThat(clients.minted.get(0).closes)
                .as("a client the enumerator never took is released by the source")
                .isOne();
    }

    /**
     * The <em>factory</em> travels; the client does not, and must not.
     *
     * <p>A serializable client could be parked on the configuration again, which is the shape
     * {@code docs/adr/0128} forbids and the one this source still had. The second assertion is what
     * would notice it coming back.
     */
    @Test
    void theFactoryTravelsInTheJobGraphAndTheClientDoesNot() throws Exception {
        DefaultChangeStreamCoordinatorClientFactory factory =
                new DefaultChangeStreamCoordinatorClientFactory(
                        TableDestination.of("project", "instance", "table"),
                        "single-cluster",
                        null);

        DefaultChangeStreamCoordinatorClientFactory back =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(factory), getClass().getClassLoader());

        assertThat(back.create()).isInstanceOf(DefaultChangeStreamCoordinatorClient.class);
        assertThat(java.io.Serializable.class.isAssignableFrom(ChangeStreamCoordinatorClient.class))
                .as("a serializable client could be parked on the configuration again")
                .isFalse();
    }

    /** Collects the mutation token; nothing here deserializes anything real. */
    private static final class TokenDeserializer
            implements BigtableChangeStreamDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(BigtableChangeStreamMutation mutation, Collector<String> out) {
            out.collect(mutation.getToken());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

    /** Mints recording clients, so a test can count how many an enumerator was given. */
    private static final class RecordingFactory implements ChangeStreamCoordinatorClientFactory {

        private static final long serialVersionUID = 1L;

        // Not round-tripped by any test; a factory that ever is needs the lazy accessor the
        // Scripted* doubles use, because a transient field deserializes to null.
        private final transient List<RecordingClient> minted = new ArrayList<>();

        @Override
        public ChangeStreamCoordinatorClient create() {
            RecordingClient client = new RecordingClient();
            minted.add(client);
            return client;
        }
    }

    /** Answers one partition and counts its own closes; nothing here reaches a service. */
    private static final class RecordingClient implements ChangeStreamCoordinatorClient {

        private int closes;

        @Override
        public void validateSingleClusterAppProfile() {}

        @Override
        public Duration retention() {
            return Duration.ofDays(7);
        }

        @Override
        public List<ByteStringRange> generateInitialPartitions() {
            return Collections.singletonList(ByteStringRange.unbounded());
        }

        @Override
        public void close() {
            closes++;
        }
    }
}
