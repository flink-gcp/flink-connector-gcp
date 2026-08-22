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

package io.github.flink.gcp.connector.base.options;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

/**
 * The check a configured value earns by being a component of a resource path this project composes.
 *
 * <p>Separate from {@link OptionChecks}, which is about a {@link java.time.Duration} and says so.
 * The argument here is its own, which is what that class asks of a new check: a connector builds
 * {@code projects/{project}/topics/{topic}} and its siblings by concatenation, so a {@code '/'}
 * inside a component does not fail — it silently addresses a <em>different</em> resource, and the
 * service answers for a name the user never typed. Leading or trailing whitespace is rejected in
 * the same breath because it is never meant, and because a name is compared and logged far from
 * where it was set.
 *
 * <p>The rule is deliberately narrow, and two neighbouring shapes fall outside it. A value that
 * <em>is</em> a full resource path — Pub/Sub's {@code kmsKeyName}, spelled {@code
 * projects/…/cryptoKeys/…} — carries the separator by definition. And a value the connector only
 * forwards for the service to resolve, such as {@code appProfileId}, is checked for presence and
 * blankness alone: what characters that service accepts in a name is its answer, and a copy of its
 * rules kept here would refuse a name it would have taken. ADR-0127 carries the survey and the
 * measurement behind both boundaries.
 *
 * <p>The three messages are the wording the six private copies this replaces already had, kept
 * verbatim so a rejection reads the same as it did before.
 */
@Internal
public final class ResourceNames {

    private ResourceNames() {}

    /**
     * Returns the value, having checked it can be concatenated into a resource path unchanged.
     *
     * @param value the configured component to check
     * @param name the option name, for the failure message
     * @return the value
     */
    public static String checkComponent(String value, String name) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(value), "%s must not be blank", name);
        Preconditions.checkArgument(
                value.equals(value.trim()),
                "%s must not have leading or trailing whitespace: '%s'",
                name,
                value);
        Preconditions.checkArgument(
                value.indexOf('/') < 0, "%s must not contain '/': '%s'", name, value);
        return value;
    }

    /**
     * Returns the value, having checked it is present and not blank.
     *
     * <p>For a value the connector forwards rather than composes, where the service owns which
     * characters its names may hold. Blankness is still ours: an empty or whitespace-only value is
     * a configuration mistake in every service, and rejecting it at the setter names the option
     * rather than leaving a remote error to be traced back.
     *
     * @param value the configured value to check
     * @param name the option name, for the failure message
     * @return the value
     */
    public static String checkNotBlank(String value, String name) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(value), "%s must not be blank", name);
        return value;
    }
}
