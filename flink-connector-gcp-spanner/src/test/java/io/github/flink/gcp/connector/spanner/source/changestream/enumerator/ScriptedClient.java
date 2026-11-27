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

package io.github.flink.gcp.connector.spanner.source.changestream.enumerator;

import org.apache.flink.util.function.ThrowingRunnable;

import java.time.Duration;

/**
 * A coordinator client whose initialization and close a test scripts, counting the calls to each.
 */
final class ScriptedClient implements SpannerChangeStreamCoordinatorClient {

    int initializeCalls;
    int closeCalls;
    RuntimeException initializationFailure;
    Runnable duringInitialize = () -> {};
    ThrowingRunnable<Exception> duringClose = () -> {};

    @Override
    public Duration initialize() {
        initializeCalls++;
        duringInitialize.run();
        if (initializationFailure != null) {
            throw initializationFailure;
        }
        return Duration.ofDays(7);
    }

    @Override
    public void close() throws Exception {
        duringClose.run();
        closeCalls++;
    }
}
