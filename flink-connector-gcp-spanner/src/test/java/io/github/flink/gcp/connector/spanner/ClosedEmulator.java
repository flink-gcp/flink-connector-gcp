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

package io.github.flink.gcp.connector.spanner;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;

/**
 * An emulator endpoint nothing listens on, so that a client built against it fails its emulator
 * check. A port the operating system just handed out and took back, rather than a fixed one: the
 * Spanner emulator's default port may be serving on a developer's machine.
 */
public final class ClosedEmulator {

    private ClosedEmulator() {}

    /** Returns the endpoint as {@code host:port}. */
    public static String target() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return "localhost:" + socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Returns the endpoint. */
    public static EmulatorEndpoint endpoint() {
        return EmulatorEndpoint.parse(target(), "emulatorEndpoint");
    }
}
