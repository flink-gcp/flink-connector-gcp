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

package io.github.flink.gcp.connector.datastore;

import org.apache.flink.annotation.Internal;

/**
 * The Datastore API's {@code PropertyMask} path grammar, shared by the lookup's mask and a merge's:
 * a path is property names joined by dots, and a name holding a dot or a backslash must be escaped
 * ({@code datastore.proto}). Each name is written as one backquoted segment, a backquote or a
 * backslash in it escaped by a backslash, so it is always one property, never a path into an
 * embedded entity. A name holding a dot, a backquote or a backslash addressed its one property in a
 * lookup's mask on the emulator and in a merge's on the emulator and the service.
 */
@Internal
public final class PropertyPaths {

    private PropertyPaths() {}

    /**
     * Returns the property name as one path segment.
     *
     * @param name a property name, as stored
     * @return the name backquoted, its backquotes and backslashes escaped
     */
    public static String segment(String name) {
        return "`" + name.replace("\\", "\\\\").replace("`", "\\`") + "`";
    }
}
