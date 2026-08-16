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

package io.github.flink.gcp.connector.cloudtasks.sink;

import com.google.cloud.tasks.v2.AppEngineRouting;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AppEngineTargetChecks}. */
class AppEngineTargetChecksTest {

    @ParameterizedTest
    @ValueSource(strings = {"", "/", "/tasks/%E2%9C%93?source=flink&empty="})
    void acceptsValidRelativeUris(String value) {
        assertThat(AppEngineTargetChecks.checkRelativeUri(value, "relative URI")).isEqualTo(value);
    }

    @ParameterizedTest
    @ValueSource(strings = {"tasks/orders", "//worker.example/tasks", "/tasks#fragment", "/tasks%"})
    void rejectsMalformedRelativeUris(String value) {
        assertThatThrownBy(() -> AppEngineTargetChecks.checkRelativeUri(value, "relative URI"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("relative URI");
    }

    @Test
    void rejectsWhitespaceBeforeUriParsing() {
        assertThatThrownBy(
                        () ->
                                AppEngineTargetChecks.checkRelativeUri(
                                        "/tasks with space", "relative URI"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("relative URI")
                .hasMessageContaining("whitespace");
    }

    @Test
    void enforcesTheRelativeUriLengthLimit() {
        String maximumLength = "/" + "a".repeat(2082);

        assertThat(AppEngineTargetChecks.checkRelativeUri(maximumLength, "relative URI"))
                .isEqualTo(maximumLength);
        assertThatThrownBy(
                        () ->
                                AppEngineTargetChecks.checkRelativeUri(
                                        maximumLength + "a", "relative URI"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("2083");
    }

    @ParameterizedTest
    @ValueSource(strings = {"Host", " content-LENGTH ", "X-Google-Trace", "x-appengine-task"})
    void rejectsReservedHeadersCaseInsensitively(String name) {
        assertThatThrownBy(() -> AppEngineTargetChecks.checkHeaderName(name))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(name)
                .hasMessageContaining("cannot be overridden");
    }

    @Test
    void acceptsCallerOwnedHeaders() {
        AppEngineTargetChecks.checkHeaderName("X-Request-Id");
        AppEngineTargetChecks.checkHeaderName("Content-Type");
    }

    @Test
    void normalizesAbsentAndEmptyRouting() {
        assertThat(AppEngineTargetChecks.checkAndNormalizeRouting(null, "routing")).isNull();
        assertThat(
                        AppEngineTargetChecks.checkAndNormalizeRouting(
                                AppEngineRouting.getDefaultInstance(), "routing"))
                .isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"service", "version", "instance"})
    void preservesEachConfiguredRoutingSelector(String selector) {
        AppEngineRouting.Builder routing = AppEngineRouting.newBuilder();
        switch (selector) {
            case "service":
                routing.setService("worker");
                break;
            case "version":
                routing.setVersion("v2");
                break;
            case "instance":
                routing.setInstance("instance-3");
                break;
            default:
                throw new AssertionError("Unexpected selector: " + selector);
        }
        AppEngineRouting configured = routing.build();

        assertThat(AppEngineTargetChecks.checkAndNormalizeRouting(configured, "routing"))
                .isSameAs(configured);
    }

    @Test
    void rejectsTheOutputOnlyRoutingHost() {
        AppEngineRouting routing =
                AppEngineRouting.newBuilder()
                        .setService("worker")
                        .setHost("worker.example")
                        .build();

        assertThatThrownBy(() -> AppEngineTargetChecks.checkAndNormalizeRouting(routing, "routing"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("routing")
                .hasMessageContaining("output-only");
    }
}
