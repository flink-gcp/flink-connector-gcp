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

package io.github.flink.gcp.connector.cloudtasks.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.cloud.tasks.v2.AppEngineRouting;

import javax.annotation.Nullable;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;

/** Checks and normalizes App Engine request fields shared by all sink APIs. */
@Internal
public final class AppEngineTargetChecks {

    private static final int MAX_RELATIVE_URI_LENGTH = 2083;

    /** Validates an App Engine relative URI. */
    public static String checkRelativeUri(String value, String name) {
        Preconditions.checkNotNull(value, "%s must not be null", name);
        Preconditions.checkArgument(
                value.length() <= MAX_RELATIVE_URI_LENGTH,
                "%s must be at most %s characters",
                name,
                MAX_RELATIVE_URI_LENGTH);
        if (value.isEmpty()) {
            return value;
        }
        Preconditions.checkArgument(
                value.startsWith("/"), "%s must be empty or begin with '/': '%s'", name, value);
        Preconditions.checkArgument(
                value.chars().noneMatch(Character::isWhitespace),
                "%s must not contain whitespace: '%s'",
                name,
                value);
        final URI parsed;
        try {
            parsed = new URI(value);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                    name + " must be a valid HTTP relative URI: '" + value + "'");
        }
        Preconditions.checkArgument(
                !parsed.isAbsolute()
                        && parsed.getRawAuthority() == null
                        && parsed.getRawFragment() == null,
                "%s must contain only a path and optional query: '%s'",
                name,
                value);
        return value;
    }

    /** Rejects headers that Cloud Tasks or App Engine owns. */
    public static void checkHeaderName(String name) {
        String normalized = name.trim().toLowerCase(Locale.ROOT);
        Preconditions.checkArgument(
                !normalized.equals("host")
                        && !normalized.equals("content-length")
                        && !normalized.startsWith("x-google-")
                        && !normalized.startsWith("x-appengine-"),
                "App Engine header '%s' is set by Cloud Tasks and cannot be overridden",
                name);
    }

    /** Removes an empty routing value and rejects its output-only host field. */
    @Nullable
    public static AppEngineRouting checkAndNormalizeRouting(
            @Nullable AppEngineRouting value, String name) {
        if (value == null) {
            return null;
        }
        Preconditions.checkArgument(
                value.getHost().isEmpty(),
                "%s must not set AppEngineRouting.host because host is output-only",
                name);
        if (value.getService().isEmpty()
                && value.getVersion().isEmpty()
                && value.getInstance().isEmpty()) {
            return null;
        }
        return value;
    }

    private AppEngineTargetChecks() {}
}
