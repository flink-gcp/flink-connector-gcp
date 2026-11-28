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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Objects;

/**
 * The {@code host:port} of a Google Cloud emulator, parsed and validated on the client rather than
 * on a TaskManager after the job has been submitted (issue #235). A DataStream caller meets the
 * parse in the builder setter that accepts the value. A SQL caller meets it when the statement over
 * the table is planned, in the table factory of every connector: Bigtable's and Spanner's because
 * their lookup path would otherwise carry the string to a TaskManager (issue #1009), and the other
 * three because reaching the parse through a builder setter answered a caller who had written an
 * option key by naming a setter (issue #1019).
 *
 * <p>Bigtable's and Spanner's lookup and full-cache runtimes parse it a second time when they open,
 * because they take the value through {@code @Internal} constructors that nothing else validates.
 * That call is the check behind those constructors rather than the one a SQL caller reaches. Every
 * builder setter keeps its parse for the same reason: it is the check the DataStream API meets, and
 * there the setter's name is the name the caller wrote.
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

    /** gRPC's DNS name-resolver scheme. */
    private static final String DNS_SCHEME = "dns";

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
     * <p>What this catches is the <em>shape</em> of the value, not the validity of the host. The
     * host is inspected only for an {@code http://} or {@code https://} scheme, in any case, which
     * must be followed by a host so that {@link #withoutHttpScheme()} still leaves {@code
     * host:port}; otherwise {@code !!!:8086} and {@code localhost:8086:9000} both parse, and a
     * scheme-prefixed gRPC target such as {@code dns:///localhost:8085} keeps working. Treat {@link
     * #getHost()} as an unvalidated string that the client resolves, not as a hostname this type
     * has vouched for.
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
     *     1..65535, or its host is an HTTP scheme with nothing after it
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
        String host = emulatorEndpoint.substring(0, separator);
        if (host.length() == httpSchemeLength(host)) {
            throw malformed(emulatorEndpoint, name);
        }
        return new EmulatorEndpoint(host, port);
    }

    /**
     * Returns this endpoint without a leading {@code http://} or {@code https://}, in any case,
     * which some client libraries' own emulator settings accept and gRPC would take for a
     * name-resolver scheme it does not have; a resolver scheme such as {@code dns:///}, which gRPC
     * resolves itself, is kept. {@link #parse(String, String)} has checked that a host follows the
     * scheme.
     *
     * @return the endpoint without an HTTP scheme, or this endpoint if it has none
     */
    public EmulatorEndpoint withoutHttpScheme() {
        int scheme = httpSchemeLength(host);
        return scheme == 0 ? this : new EmulatorEndpoint(host.substring(scheme), port);
    }

    /**
     * Returns the {@code host:port} this endpoint dials, for a client that takes an HTTP authority
     * rather than a gRPC target: {@link #withoutHttpScheme()}'s target, where a URI with gRPC's
     * {@code dns} scheme is reduced as gRPC's DNS resolver reads it. The first segment of its path,
     * decoded, is the name, an opaque URI such as {@code dns:host:port} being read as {@code
     * dns:///host:port}, so that a query is left out; the name is then read as {@code //name}, and
     * its host and port are the {@code host:port}, so that an encoded {@code ?} or {@code /} inside
     * the name ends it and user information before an {@code @} is left out. Thus {@code
     * dns:///host:port} and {@code dns://8.8.8.8/host:port} both give {@code host:port}. A path of
     * more than one segment is reduced to its first, which gRPC dials where it accepts such a
     * target at all (by default, {@code grpc-grpclb}'s {@code dns} provider, which the Firestore
     * module has, refuses it), and a name without a port gives its host alone, which both gRPC's
     * plaintext transport and HTTP dial on port 80. Anything else, and a name without a
     * server-based host, is returned as it is.
     *
     * @return the HTTP authority the endpoint dials: {@code host:port}, or the host alone when the
     *     name has no port, which an HTTP client takes as port 80 but a client that requires an
     *     explicit port does not accept
     */
    public String httpAuthority() {
        String target = withoutHttpScheme().getTarget();
        URI uri;
        try {
            uri = new URI(target);
        } catch (URISyntaxException e) {
            return target;
        }
        if (!DNS_SCHEME.equals(uri.getScheme())) {
            return target;
        }
        try {
            if (uri.isOpaque()) {
                // Read as dns:///<the rest>, whose path leaves out a query, which gRPC does not
                // dial.
                uri = new URI(DNS_SCHEME + ":///" + uri.getRawSchemeSpecificPart());
            }
            String rawPath = uri.getRawPath();
            if (!rawPath.startsWith("/")) {
                return target;
            }
            // The first segment, which is what gRPC dials when it accepts more than one.
            int end = rawPath.indexOf('/', 1);
            String firstSegment = end < 0 ? rawPath : rawPath.substring(0, end);
            String name = new URI(DNS_SCHEME + "://" + firstSegment).getPath().substring(1);
            // gRPC's DnsNameResolver dials the host and port of //<name> (grpc-java 1.85.1).
            URI dialled = new URI("//" + name);
            if (dialled.getHost() == null) {
                return name;
            }
            return dialled.getPort() >= 0
                    ? dialled.getHost() + ":" + dialled.getPort()
                    : dialled.getHost();
        } catch (URISyntaxException e) {
            return target;
        }
    }

    /**
     * Returns the length of the {@code http://} or {@code https://} the host starts with, or 0. The
     * scheme is matched in ASCII letter case only: a Unicode case fold would also take, say, a long
     * s ({@code U+017F}) for an {@code s}.
     */
    private static int httpSchemeLength(String host) {
        for (String scheme : new String[] {"http://", "https://"}) {
            if (host.length() >= scheme.length()
                    && host.substring(0, scheme.length()).toLowerCase(Locale.ROOT).equals(scheme)) {
                return scheme.length();
            }
        }
        return 0;
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
