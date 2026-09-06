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

package io.github.flink.gcp.connector.spanner.source.changestream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChangeStreamPartitionSplitTest {

    @ParameterizedTest
    @MethodSource("parentLists")
    void acceptsAndNormalizesParentLists(List<String> parents) throws Exception {
        ChangeStreamPartitionSplit split = split("child", parents);
        List<String> expected = parents.size() == 1 ? List.of("b") : List.of("a", "b");
        ChangeStreamPartitionSplit normalized = split("child", new ArrayList<>(expected));

        assertThat(split.getParentPartitionIds()).containsExactlyElementsOf(expected);
        assertThat(split).isEqualTo(normalized).hasSameHashCodeAs(normalized);
        assertThat(split.samePartitionDefinition(normalized)).isTrue();
        assertThatThrownBy(() -> split.getParentPartitionIds().add("other"))
                .isInstanceOf(UnsupportedOperationException.class);
        ChangeStreamPartitionSplitSerializer serializer =
                new ChangeStreamPartitionSplitSerializer();
        assertThat(serializer.serialize(split)).isEqualTo(serializer.serialize(normalized));
        assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(split)))
                .isEqualTo(split);
    }

    @Test
    void copiesMutableParents() {
        List<String> parents = new ArrayList<>(List.of("b", "a", "b"));
        ChangeStreamPartitionSplit split = split("child", parents);
        parents.clear();
        parents.add("other");

        assertThat(split.getParentPartitionIds()).containsExactly("a", "b");
        assertThat(split).isEqualTo(split("child", Arrays.asList("a", "b")));
    }

    @Test
    void onlyTheInitialSplitAcceptsEmptyParents() {
        for (List<String> empty :
                Arrays.asList(
                        List.<String>of(),
                        List.<String>copyOf(new ArrayList<>()),
                        new ArrayList<String>())) {
            ChangeStreamPartitionSplit initial = split(null, empty);
            assertThat(initial.getParentPartitionIds()).isEmpty();
            assertThat(initial)
                    .isEqualTo(ChangeStreamPartitionSplit.initial(Instant.EPOCH, null, 2_000));
            assertThatThrownBy(() -> split("child", empty))
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("a token partition must have at least one parent");
        }
        assertThatThrownBy(() -> split(null, List.of("parent")))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("the initial split must not have parents");
    }

    @Test
    void rejectsInvalidParentsWithExistingMessages() {
        for (String token : Arrays.asList(null, "child")) {
            assertThatThrownBy(() -> split(token, null))
                    .isExactlyInstanceOf(NullPointerException.class)
                    .hasMessage("parentPartitionIds must not be null");
            assertThatThrownBy(() -> split(token, Arrays.asList("", null)))
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("parentPartitionIds must not contain null");
            assertThatThrownBy(() -> split(token, List.of("parent", "")))
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("parentPartitionIds must not contain empty ids");
        }
        assertThatThrownBy(() -> split("", List.of("parent")))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("partitionToken must not be empty");
    }

    private static Stream<List<String>> parentLists() {
        return Stream.of(List.of("b"), List.of("b", "a"), List.of("b", "a", "b"))
                .flatMap(
                        values ->
                                Stream.of(
                                        values,
                                        List.copyOf(new ArrayList<>(values)),
                                        new ArrayList<>(values)));
    }

    private static ChangeStreamPartitionSplit split(String token, List<String> parents) {
        return new ChangeStreamPartitionSplit(
                token,
                parents,
                Instant.EPOCH,
                null,
                2_000,
                Instant.EPOCH,
                PartitionLifecycleState.SCHEDULED,
                Instant.EPOCH);
    }
}
