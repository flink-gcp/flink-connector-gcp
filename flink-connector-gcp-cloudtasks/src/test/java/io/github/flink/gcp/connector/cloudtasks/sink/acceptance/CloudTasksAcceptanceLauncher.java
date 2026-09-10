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

package io.github.flink.gcp.connector.cloudtasks.sink.acceptance;

import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;

import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.Map;

/**
 * Executes a fixed inventory from an already compiled classpath without rebuilding during billing.
 */
public final class CloudTasksAcceptanceLauncher {
    private CloudTasksAcceptanceLauncher() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: <local|recovery|slow> <new-summary-jsonl>");
        }
        Class<?> selected;
        int expected;
        if (args[0].equals("local")) {
            selected = CloudTasksRecoveryHarnessITCase.class;
            expected = 48;
        } else if (args[0].equals("recovery")) {
            selected = CloudTasksRecoveryRealGcpITCase.class;
            expected = 56;
        } else if (args[0].equals("slow")) {
            selected = CloudTasksTombstoneRealGcpITCase.class;
            expected = 1;
        } else {
            throw new IllegalArgumentException("Unknown acceptance inventory");
        }
        var listener = new SummaryGeneratingListener();
        var request =
                LauncherDiscoveryRequestBuilder.request()
                        .selectors(DiscoverySelectors.selectClass(selected))
                        .configurationParameter("junit.jupiter.execution.parallel.enabled", "false")
                        .build();
        try (var output = new AcceptanceEvidence(Path.of(args[1]))) {
            LauncherFactory.create().execute(request, listener);
            var summary = listener.getSummary();
            summary.printTo(new PrintWriter(System.out, true));
            summary.printFailuresTo(new PrintWriter(System.err, true));
            output.record(
                    "test-summary",
                    Map.of(
                            "inventory",
                            args[0],
                            "expected",
                            expected,
                            "found",
                            summary.getTestsFoundCount(),
                            "succeeded",
                            summary.getTestsSucceededCount(),
                            "skipped",
                            summary.getTestsSkippedCount(),
                            "aborted",
                            summary.getTestsAbortedCount(),
                            "failures",
                            summary.getTotalFailureCount()));
            if (summary.getTestsFoundCount() != expected
                    || summary.getTestsSucceededCount() != expected
                    || summary.getTestsSkippedCount() != 0
                    || summary.getTestsAbortedCount() != 0
                    || summary.getTotalFailureCount() != 0) {
                throw new AssertionError("Acceptance inventory incomplete or failed");
            }
        }
    }
}
