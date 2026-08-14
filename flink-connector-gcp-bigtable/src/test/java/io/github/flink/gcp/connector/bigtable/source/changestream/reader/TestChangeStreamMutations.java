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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import io.github.flink.gcp.connector.bigtable.source.changestream.ChangeStreamMutation;

import java.io.IOException;

/** Converts pinned SDK fixtures without exposing the production converter as public API. */
public final class TestChangeStreamMutations {

    private TestChangeStreamMutations() {}

    public static ChangeStreamMutation convert(
            com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation mutation) {
        try {
            return ChangeStreamMutationConverter.convert(mutation);
        } catch (IOException e) {
            throw new AssertionError("The pinned SDK fixture must be supported", e);
        }
    }
}
