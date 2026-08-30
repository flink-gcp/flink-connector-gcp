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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.loadjob;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigquery.Schema;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import javax.annotation.Nullable;

import java.util.List;

/** Every planned job and cleanup target for one destination in a commit. */
@Internal
final class DestinationCommitPlan {

    final TableDestination destination;
    final List<PlannedLoad> loads;
    @Nullable final DestinationCopy copy;
    @Nullable Schema reconciledSchema;

    DestinationCommitPlan(
            TableDestination destination, List<PlannedLoad> loads, @Nullable DestinationCopy copy) {
        this.destination = destination;
        this.loads = loads;
        this.copy = copy;
    }
}
