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

package io.github.flink.gcp.connector.base.rpc;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Objects;

/**
 * The {@code host:port} of a Google Cloud emulator, parsed and validated once — in the builder
 * setter that accepts it, so a malformed value fails on the client rather than on a TaskManager
 * after the job has been submitted (issue #235). The exception is the Table API's lookup and
 * full-cache scan paths, which hold the option's value and parse it when the runtime opens.
 *
 * <p>Every setter that takes an emulator endpoint funnels through {@link #parse(String, String)},
 * and this type is the only form the value takes from there on: a client can therefore never be
 * handed an endpoint nothing has checked. Consumers building a channel or gax settings from a
 * target string take {@link #getTarget()}; consumers taking a host and a port separately — the
 * Bigtable client's emulator settings — take {@link #getHost()} and {@link #getPort()}.
 */
@Internal
public final class EmulatorEndpoint implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final int MIN_PORT = 1;
    private static final int MAX_PORT = 65535;

    private final String host;
    private final int port;

    private EmulatorEndpoint(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Parses an endpoint written as {@code host:port}.
     *
     * <p>The value is taken exactly as given: it is not trimmed, and any whitespace in it is a
     * rejection rather than something to strip, because a trimmed value would silently be a
     * different endpoint from the one that was configured. The host is kept verbatim, split at the
     * <em>last</em> colon, so an IPv6 literal keeps its brackets and reaches the client the way it
     * always has.
     *
     * <p>What this catches is the <em>shape</em> of the value, not the validity of the host: the
     * host is never inspected, so {@code !!!:8086} and {@code localhost:8086:9000} both parse, and
     * a scheme-prefixed gRPC target such as {@code dns:///localhost:8085} keeps working. Treat
     * {@link #getHost()} as an unvalidated string that the client resolves, not as a hostname this
     * type has vouched for.
     *
     * <p>Both rejections name the setting the caller was given, and the name is required rather
     * than defaulted: a default is how BigQuery's two {@code emulatorRestEndpoint(String)} setters
     * spent their life naming a setter their caller may never have touched (issue #895).
     *
     * @param emulatorEndpoint the endpoint as {@code host:port}
     * @param name the setting to name in the failure messages: the setter a DataStream caller used,
     *     or the option key a SQL caller wrote
     * @return the parsed endpoint
     * @throws NullPointerException if the endpoint is {@code null}
     * @throws IllegalArgumentException if the endpoint is not {@code host:port} with a port in
     *     1..65535
     */
    public static EmulatorEndpoint parse(String emulatorEndpoint, String name) {
        Preconditions.checkNotNull(emulatorEndpoint, "%s must not be null", name);
        int separator = emulatorEndpoint.lastIndexOf(':');
        if (separator <= 0
                || separator == emulatorEndpoint.length() - 1
                || containsWhitespace(emulatorEndpoint)) {
            throw malformed(emulatorEndpoint, name);
        }
        String digits = emulatorEndpoint.substring(separator + 1);
        // Integer.parseInt on its own would accept a sign and any Unicode decimal digit; a port is
        // neither, and '+8086' reaching a client as 8086 is the kind of typo this parse exists for.
        for (int i = 0; i < digits.length(); i++) {
            char c = digits.charAt(i);
            if (c < '0' || c > '9') {
                throw malformed(emulatorEndpoint, name);
            }
        }
        int port;
        try {
            port = Integer.parseInt(digits);
        } catch (NumberFormatException e) {
            // More digits than an int holds; the range check below never sees it.
            throw malformed(emulatorEndpoint, name);
        }
        if (port < MIN_PORT || port > MAX_PORT) {
            throw malformed(emulatorEndpoint, name);
        }
        return new EmulatorEndpoint(emulatorEndpoint.substring(0, separator), port);
    }

    /** Returns the host, exactly as it was written. */
    public String getHost() {
        return host;
    }

    /** Returns the port. */
    public int getPort() {
        return port;
    }

    /**
     * Returns the endpoint as {@code host:port} — the form a gRPC channel target and a gax endpoint
     * setting take, and the form it was parsed from.
     */
    public String getTarget() {
        return host + ":" + port;
    }

    private static boolean containsWhitespace(String value) {
        for (int i = 0; i < value.length(); i++) {
            if (Character.isWhitespace(value.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    private static IllegalArgumentException malformed(String emulatorEndpoint, String name) {
        return new IllegalArgumentException(
                name + " must be host:port, was '" + emulatorEndpoint + "'");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EmulatorEndpoint that = (EmulatorEndpoint) o;
        return port == that.port && host.equals(that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    @Override
    public String toString() {
        return getTarget();
    }
}
