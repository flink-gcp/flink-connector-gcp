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

package io.github.flink.gcp.connector.bigtable.source.changestream.enumerator;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

final class ScriptedChangeStreamCoordinatorClient implements ChangeStreamCoordinatorClient {

    private static final long serialVersionUID = 1L;

    private final List<ByteStringRange> partitions;
    private final Duration retention;
    private int validationCalls;
    private int generationCalls;
    private int retentionCalls;
    private int closeCalls;

    static ScriptedChangeStreamCoordinatorClient with(ByteStringRange... partitions) {
        return new ScriptedChangeStreamCoordinatorClient(
                Duration.ofDays(7), Arrays.asList(partitions));
    }

    ScriptedChangeStreamCoordinatorClient(Duration retention, List<ByteStringRange> partitions) {
        this.retention = retention;
        this.partitions = new ArrayList<>(partitions);
    }

    @Override
    public void validateSingleClusterAppProfile() {
        validationCalls++;
    }

    @Override
    public Duration retention() {
        retentionCalls++;
        return retention;
    }

    @Override
    public List<ByteStringRange> generateInitialPartitions() {
        generationCalls++;
        return new ArrayList<>(partitions);
    }

    @Override
    public void close() {
        closeCalls++;
    }

    int validationCalls() {
        return validationCalls;
    }

    int generationCalls() {
        return generationCalls;
    }

    int retentionCalls() {
        return retentionCalls;
    }

    int closeCalls() {
        return closeCalls;
    }
}
