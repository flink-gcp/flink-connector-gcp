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
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChildPartitionsEventTest {

    @ParameterizedTest
    @MethodSource("childLists")
    void acceptsChildListsInEncounterOrder(List<ChildPartitionsEvent.ChildPartition> children) {
        ChildPartitionsEvent event = new ChildPartitionsEvent("parent", Instant.EPOCH, children);

        assertThat(event.getParentSplitId()).isEqualTo("parent");
        assertThat(event.getStartTimestamp()).isEqualTo(Instant.EPOCH);
        assertThat(event.getChildren()).containsExactlyElementsOf(children);
        assertThatThrownBy(() -> event.getChildren().add(child("other")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void copiesMutableChildren() {
        ChildPartitionsEvent.ChildPartition first = child("b");
        ChildPartitionsEvent.ChildPartition second = child("a");
        List<ChildPartitionsEvent.ChildPartition> children =
                new ArrayList<>(List.of(first, second, first));
        ChildPartitionsEvent event = new ChildPartitionsEvent("parent", Instant.EPOCH, children);
        children.clear();
        children.add(null);

        assertThat(event.getChildren()).containsExactly(first, second, first);
    }

    @ParameterizedTest
    @MethodSource("childrenContainingNull")
    void rejectsNullChildren(List<ChildPartitionsEvent.ChildPartition> children) {
        assertThatThrownBy(() -> new ChildPartitionsEvent("parent", Instant.EPOCH, children))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("children must not contain null");
    }

    @Test
    void rejectsNullOrEmptyChildListsWithExistingMessages() {
        assertThatThrownBy(() -> new ChildPartitionsEvent("parent", Instant.EPOCH, null))
                .isExactlyInstanceOf(NullPointerException.class)
                .hasMessage("children must not be null");
        for (List<ChildPartitionsEvent.ChildPartition> empty :
                Arrays.asList(
                        List.<ChildPartitionsEvent.ChildPartition>of(),
                        List.<ChildPartitionsEvent.ChildPartition>copyOf(new ArrayList<>()),
                        new ArrayList<ChildPartitionsEvent.ChildPartition>())) {
            assertThatThrownBy(() -> new ChildPartitionsEvent("parent", Instant.EPOCH, empty))
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("children must not be empty");
        }
    }

    @ParameterizedTest
    @MethodSource("parentLists")
    void acceptsAndNormalizesParentLists(List<String> parents) {
        ChildPartitionsEvent.ChildPartition child =
                new ChildPartitionsEvent.ChildPartition("child", parents);

        assertThat(child.getToken()).isEqualTo("child");
        assertThat(child.getParentPartitionIds())
                .containsExactlyElementsOf(parents.size() == 1 ? List.of("b") : List.of("a", "b"));
        assertThatThrownBy(() -> child.getParentPartitionIds().add("other"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void copiesMutableParents() {
        List<String> parents = new ArrayList<>(List.of("b", "a", "b"));
        ChildPartitionsEvent.ChildPartition child =
                new ChildPartitionsEvent.ChildPartition("child", parents);
        parents.clear();
        parents.add("other");

        assertThat(child.getParentPartitionIds()).containsExactly("a", "b");
    }

    @Test
    void rejectsInvalidParentsWithExistingMessages() {
        assertThatThrownBy(() -> new ChildPartitionsEvent.ChildPartition("child", null))
                .isExactlyInstanceOf(NullPointerException.class)
                .hasMessage("parentPartitionIds must not be null");
        for (List<String> empty :
                Arrays.asList(
                        List.<String>of(),
                        List.<String>copyOf(new ArrayList<>()),
                        new ArrayList<String>())) {
            assertThatThrownBy(() -> new ChildPartitionsEvent.ChildPartition("child", empty))
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("parentPartitionIds must not be empty");
        }
        assertThatThrownBy(
                        () ->
                                new ChildPartitionsEvent.ChildPartition(
                                        "child", Arrays.asList("", null)))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("parentPartitionIds must not contain null");
        assertThatThrownBy(
                        () ->
                                new ChildPartitionsEvent.ChildPartition(
                                        "child", List.of("parent", "")))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("parentPartitionIds must not contain empty ids");
    }

    private static Stream<List<ChildPartitionsEvent.ChildPartition>> childLists() {
        ChildPartitionsEvent.ChildPartition first = child("b");
        ChildPartitionsEvent.ChildPartition second = child("a");
        return Stream.of(List.of(first), List.of(first, second), List.of(first, second, first))
                .flatMap(
                        values ->
                                Stream.of(
                                        values,
                                        List.copyOf(new ArrayList<>(values)),
                                        new ArrayList<>(values)));
    }

    private static Stream<List<ChildPartitionsEvent.ChildPartition>> childrenContainingNull() {
        ChildPartitionsEvent.ChildPartition child = child("child");
        return Stream.of(
                        Collections.<ChildPartitionsEvent.ChildPartition>singletonList(null),
                        Arrays.asList(null, child),
                        Arrays.asList(child, null),
                        Arrays.asList(child, null, child))
                .flatMap(
                        values ->
                                Stream.of(
                                        new ArrayList<>(values),
                                        Collections.unmodifiableList(new ArrayList<>(values))));
    }

    private static ChildPartitionsEvent.ChildPartition child(String token) {
        return new ChildPartitionsEvent.ChildPartition(token, List.of("parent"));
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
}
