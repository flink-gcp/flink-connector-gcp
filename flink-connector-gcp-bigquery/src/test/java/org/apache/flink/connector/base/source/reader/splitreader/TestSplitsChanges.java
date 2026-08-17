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

package org.apache.flink.connector.base.source.reader.splitreader;

import java.util.List;

/**
 * Mints the {@link SplitsChange} shape no shipped Flink version has, from inside the package its
 * constructor is scoped to (the vendor-package pattern of {@code TestJobs}, {@code docs/adr/0067}).
 *
 * <p>{@code SplitsChange} has one package-private constructor and two shipped subclasses, so a
 * reader's arm for an unknown shape is unreachable from a connector's own packages. The arm exists
 * because Flink may add a third shape, and a reader that silently ignored one would drop or retain
 * splits at random; this mint is what lets a test hold that promise today.
 */
public final class TestSplitsChanges {

    private TestSplitsChanges() {}

    /** A {@code SplitsChange} that is neither an addition nor a removal. */
    public static <T> SplitsChange<T> unknown(List<T> splits) {
        return new SplitsChange<T>(splits) {};
    }
}
