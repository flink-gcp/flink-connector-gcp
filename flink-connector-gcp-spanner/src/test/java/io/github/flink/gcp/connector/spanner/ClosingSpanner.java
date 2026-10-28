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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Spanner;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;

import javax.annotation.Nullable;

import java.lang.reflect.Proxy;

/**
 * A {@link Spanner} handle that serves database clients nothing can be read through, and records on
 * its close whether the client metrics it watches were still open: what a close that runs in the
 * wrong order would change.
 */
public final class ClosingSpanner {

    private final ClientMetrics watched;
    private final Spanner proxy;
    private int closes;
    @Nullable private Boolean metricsOpenAtClose;

    /**
     * Creates the handle.
     *
     * @param watched the client metrics whose state the handle's close records
     */
    public ClosingSpanner(ClientMetrics watched) {
        this.watched = watched;
        this.proxy =
                (Spanner)
                        Proxy.newProxyInstance(
                                Spanner.class.getClassLoader(),
                                new Class<?>[] {Spanner.class},
                                (self, method, args) -> {
                                    switch (method.getName()) {
                                        case "getDatabaseClient":
                                            return unusableDatabaseClient();
                                        case "close":
                                            if (metricsOpenAtClose == null) {
                                                metricsOpenAtClose =
                                                        ClientMetricsProbe.isOpen(watched);
                                            }
                                            closes++;
                                            return null;
                                        case "isClosed":
                                            return closes > 0;
                                        case "toString":
                                            return "ClosingSpanner";
                                        case "hashCode":
                                            return System.identityHashCode(self);
                                        case "equals":
                                            return self == args[0];
                                        default:
                                            throw new UnsupportedOperationException(
                                                    method.getName());
                                    }
                                });
    }

    /** Returns the handle. */
    public Spanner spanner() {
        return proxy;
    }

    /** Returns how many times the handle was closed. */
    public int closes() {
        return closes;
    }

    /** Returns whether the watched client metrics were open at the first close, or null. */
    @Nullable
    public Boolean metricsOpenAtClose() {
        return metricsOpenAtClose;
    }

    private static DatabaseClient unusableDatabaseClient() {
        return (DatabaseClient)
                Proxy.newProxyInstance(
                        DatabaseClient.class.getClassLoader(),
                        new Class<?>[] {DatabaseClient.class},
                        (self, method, args) -> {
                            if ("toString".equals(method.getName())) {
                                return "UnusableDatabaseClient";
                            }
                            throw new UnsupportedOperationException(method.getName());
                        });
    }
}
