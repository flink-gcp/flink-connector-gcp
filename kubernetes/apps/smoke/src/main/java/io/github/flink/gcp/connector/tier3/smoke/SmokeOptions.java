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

package io.github.flink.gcp.connector.tier3.smoke;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ParameterTool;

import java.io.Serializable;
import java.util.Set;

/** Command-line inputs for one bounded amount of smoke data. */
@Internal
final class SmokeOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    final String runId;
    final String phase;
    final long records;
    final double recordsPerSecond;
    final boolean requireRestored;

    private SmokeOptions(ParameterTool parameters) {
        Set<String> names =
                Set.of("run-id", "phase", "records", "records-per-second", "require-restored");
        for (String name : parameters.toMap().keySet()) {
            if (!names.contains(name)) {
                throw new IllegalArgumentException("Unknown smoke argument: --" + name);
            }
        }
        runId = parameters.getRequired("run-id");
        // Match the CUE label grammar and keep each audit field on one line.
        if (!runId.matches("^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$")) {
            throw new IllegalArgumentException("--run-id must match the Tier-3 run label grammar");
        }
        phase = parameters.get("phase", "initial");
        if (!phase.equals("initial") && !phase.equals("upgrade")) {
            throw new IllegalArgumentException("--phase must be initial or upgrade");
        }
        records = parameters.getLong("records", 18_000L);
        if (records < 1 || records > 18_000) {
            throw new IllegalArgumentException("--records must be between 1 and 18000");
        }
        recordsPerSecond = parameters.getDouble("records-per-second", 10);
        if (!Double.isFinite(recordsPerSecond) || recordsPerSecond <= 0 || recordsPerSecond > 10) {
            throw new IllegalArgumentException(
                    "--records-per-second must be greater than 0 and at most 10");
        }
        String restored = parameters.get("require-restored", "false");
        if (!restored.equals("true") && !restored.equals("false")) {
            throw new IllegalArgumentException("--require-restored must be true or false");
        }
        requireRestored = Boolean.parseBoolean(restored);
    }

    static SmokeOptions parse(String... args) {
        return new SmokeOptions(ParameterTool.fromArgs(args));
    }
}
