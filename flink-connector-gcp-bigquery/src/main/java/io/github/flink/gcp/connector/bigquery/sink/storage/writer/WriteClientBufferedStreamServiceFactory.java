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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Default {@link BufferedStreamServiceFactory} creating {@link WriteClientBufferedStreamService}s.
 *
 * <p>The emulator endpoint is held here rather than added to {@link
 * BufferedStreamServiceFactory#create}: it says where this sink writes, which is a property of the
 * factory, not of the stream being opened — and the SPI's other implementations are fakes that open
 * no client at all.
 */
@Internal
public final class WriteClientBufferedStreamServiceFactory implements BufferedStreamServiceFactory {

    private static final long serialVersionUID = 1L;

    @Nullable private final EmulatorEndpoint emulatorEndpoint;
    @Nullable private final String serviceAccountKeyFile;

    /** Creates a factory writing to the production service. */
    public WriteClientBufferedStreamServiceFactory() {
        this(null, null);
    }

    /**
     * Creates a factory.
     *
     * @param emulatorEndpoint the emulator to write to, or {@code null} for the production service
     */
    public WriteClientBufferedStreamServiceFactory(@Nullable EmulatorEndpoint emulatorEndpoint) {
        this(null, emulatorEndpoint);
    }

    /** Creates a factory with optional runtime-loaded production credentials. */
    public WriteClientBufferedStreamServiceFactory(
            @Nullable String serviceAccountKeyFile, @Nullable EmulatorEndpoint emulatorEndpoint) {
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    /** Returns the emulator this factory writes to, or {@code null} for the production service. */
    @Nullable
    public EmulatorEndpoint getEmulatorEndpoint() {
        return emulatorEndpoint;
    }

    @Override
    public BufferedStreamService create(String location, BufferedStreamOptions options)
            throws IOException {
        return new WriteClientBufferedStreamService(
                location, options, serviceAccountKeyFile, emulatorEndpoint);
    }
}
