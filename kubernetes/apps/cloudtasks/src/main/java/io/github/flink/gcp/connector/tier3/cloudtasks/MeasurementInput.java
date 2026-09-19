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

package io.github.flink.gcp.connector.tier3.cloudtasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.io.IOException;
import java.util.UUID;

/** Emits the same finite sequence while separately recording source mapping boundaries. */
@Internal
final class MeasurementInput implements GeneratorFunction<Long, Long> {
    private static final long serialVersionUID = 1L;
    private final MeasurementOptions options;
    private transient UUID incarnation;
    private transient MeasurementReceipts.Output output;

    MeasurementInput(MeasurementOptions options) {
        this.options = options;
    }

    MeasurementInput(MeasurementOptions options, MeasurementReceipts.Output output) {
        this(options);
        this.output = output;
    }

    @Override
    public Long map(Long sequence) throws IOException {
        if (options.windowed()) {
            if (incarnation == null) {
                incarnation = UUID.randomUUID();
                if (output == null) {
                    output = MeasurementReceipts.storage(options);
                }
                boundary("start", sequence);
            }
            if (sequence == options.records - 1) {
                boundary("last-mapped", sequence);
            }
        }
        return sequence;
    }

    private void boundary(String phase, long sequence) throws IOException {
        output.write(
                "source-" + incarnation + "-" + phase,
                MeasurementReceipts.identity(options, "source", incarnation)
                        + ",\"sequence\":"
                        + sequence
                        + ",\"records\":"
                        + options.records
                        + ",\"warmup_seconds\":"
                        + options.warmupSeconds
                        + ",\"observation_seconds\":"
                        + options.observationSeconds
                        + ",\"offered_rate\":"
                        + options.offeredRate
                        + ",\"wall_millis\":"
                        + System.currentTimeMillis()
                        + ",\"monotonic_nanos\":"
                        + System.nanoTime()
                        + "}");
    }
}
