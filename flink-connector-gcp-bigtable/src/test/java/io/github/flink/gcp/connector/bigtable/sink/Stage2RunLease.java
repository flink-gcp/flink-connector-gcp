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

package io.github.flink.gcp.connector.bigtable.sink;

import io.github.flink.gcp.connector.bigtable.TableDestination;

import java.io.IOException;
import java.nio.file.Path;

/** Admission and reservations shared by the legacy instance lease and retained campaign workers. */
interface Stage2RunLease {
    void requireLive() throws IOException;

    void requireTarget(TableDestination destination) throws IOException;

    long reserve(long attempts, long bytes) throws IOException;

    void reserveRead(long bytes) throws IOException;

    TableDestination table(String name) throws IOException;

    Path workDirectory();

    default Path workRoot() {
        return workDirectory();
    }

    Path evidenceDirectory();

    void requireWindow(long observationMillis) throws IOException;

    static Stage2RunLease legacy(Stage2Lease lease) {
        if (lease == null) {
            return null;
        }
        return new Stage2RunLease() {
            @Override
            public void requireLive() throws IOException {
                lease.requireLive();
            }

            @Override
            public void requireTarget(TableDestination destination) throws IOException {
                lease.requireTarget(destination);
            }

            @Override
            public long reserve(long attempts, long bytes) throws IOException {
                return lease.reserve(attempts, bytes);
            }

            @Override
            public void reserveRead(long bytes) throws IOException {
                lease.reserveRead(bytes);
            }

            @Override
            public TableDestination table(String name) throws IOException {
                return lease.table(name);
            }

            @Override
            public Path workDirectory() {
                return lease.work;
            }

            @Override
            public Path evidenceDirectory() {
                return lease.manifest.getParent();
            }

            @Override
            public void requireWindow(long observationMillis) throws IOException {
                requireLive();
                if (Math.addExact(System.currentTimeMillis(), observationMillis)
                        > Math.addExact(lease.startedAt(), 45 * 60_000L)) {
                    throw new IOException(
                            "Full observation no longer fits the admission lease; leave it unmeasured");
                }
            }
        };
    }
}
