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

package io.github.flink.gcp.connector.testutils;

import org.apache.flink.annotation.Internal;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Ports;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.core.CreateContainerCmdModifier;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * Publishes every container's ports on the loopback address the Docker host resolves to, rather
 * than on the wildcard address, so a host-port collision with an unrelated local process cannot
 * form (ADR-0132).
 *
 * <p>Docker's default publish is the wildcard address, and that <em>coexists</em> with a process
 * already holding {@code 127.0.0.1:<port>}: the more specific bind wins, so a client resolving
 * {@code localhost} to the IPv4 loopback reaches the stranger while the container sits there
 * healthy. Asking for a loopback address without a port makes the kernel pick a port that is free
 * <em>on that address</em>, so the collision has nowhere to form.
 *
 * <p>This is registered through {@code META-INF/services} rather than applied per harness because
 * {@link org.testcontainers.containers.GenericContainer} loads the SPI <em>per container
 * instance</em>, which reaches containers no harness in this repository constructs —
 * testcontainers' own Ryuk reaper above all. Ryuk is contacted before any harness runs, so a
 * collision on its port fails a whole module's integration tests with a message naming no connector
 * at all. No per-harness change can reach that.
 *
 * <p><b>What this does not cost.</b> {@code curl http://localhost:<port>} still works. #1021
 * expected it to break, because {@code curl} prefers {@code ::1} and nothing is bound there any
 * more — measured 2026-08-22 with curl 8.7.1 on macOS, it tries {@code ::1}, is refused
 * <em>immediately</em>, falls back to {@code 127.0.0.1} and connects. Prefer {@code curl
 * http://127.0.0.1:<port>} anyway: the fallback rests on that refusal being instant, and if
 * something <em>is</em> listening on {@code ::1:<port>} curl reaches it rather than the container —
 * the same shadowing hazard, moved to IPv6, and now reachable only by a hand-run {@code curl}
 * rather than by the JVM.
 *
 * <p><b>Only a loopback Docker host, and the guard is an address test.</b> Against a daemon reached
 * at a routable address — {@code DOCKER_HOST=tcp://…}, Testcontainers Cloud — a loopback address is
 * the <em>daemon's</em>, which the test JVM cannot reach, so the wildcard publish is left alone.
 * What that test cannot see is a daemon reached <em>at</em> a loopback address but running
 * elsewhere: an SSH tunnel on {@code tcp://127.0.0.1:2375}, or a VM whose port forwarder only
 * carries wildcard-bound guest ports. There the modifier engages and binds inside the daemon, and
 * the connection is refused. Nothing distinguishes that from a local daemon — a local one exposed
 * over {@code tcp://127.0.0.1} is a setup this <em>should</em> serve — so the escape is explicit
 * rather than inferred: pass {@code -Dflink.gcp.tests.loopback-publish=false}. Maven forwards a
 * command-line {@code -D} into the surefire fork, and {@code .mvn/maven.config} or {@code
 * MAVEN_OPTS} carries it for a shell that sets no flags per run.
 *
 * <p>That restores testcontainers' own wildcard publish. The property is read per container rather
 * than once, so clearing it mid-run re-engages the rewrite for later containers. Docker Desktop for
 * macOS — itself VM-backed — is measured working without it; colima and Rancher Desktop are not
 * measured, which is what the escape is for.
 *
 * <p>ADR-0132 carries the evidence, the declined alternatives, and the dependency-bump tripwire
 * that this class's survival rests on.
 */
@Internal
public final class LoopbackPortPublisher implements CreateContainerCmdModifier {

    /**
     * System property whose value {@code false} switches the rewrite off. Dotted lowercase with a
     * hyphen, which is Flink's own configuration-key style; this repository's one other property,
     * {@code test.excluded.groups}, is lowercase-dotted too.
     */
    static final String LOOPBACK_PUBLISH_PROPERTY = "flink.gcp.tests.loopback-publish";

    /** Required by {@link java.util.ServiceLoader}, which is the only intended caller. */
    public LoopbackPortPublisher() {}

    @Override
    public CreateContainerCmd modify(CreateContainerCmd createContainerCmd) {
        // Before the Docker host is resolved, so the opt-out costs nothing and — the reason it is
        // here rather than inside the seam — so a test can drive this very method: with the
        // property set it returns without touching testcontainers' provider strategy, which is
        // what pins the constant's spelling.
        if (isDisabled(System.getProperty(LOOPBACK_PUBLISH_PROPERTY))) {
            return createContainerCmd;
        }
        // The strategy is resolved before any container is created, so this neither initialises
        // nor re-enters anything: testcontainers has already logged the address by this point.
        return modify(createContainerCmd, DockerClientFactory.instance().dockerHostIpAddress());
    }

    /**
     * Whether an explicit opt-out is in force.
     *
     * <p>Only {@code false} disables, case-insensitively and after stripping — not {@link
     * Boolean#parseBoolean}, which reads every other spelling as {@code false} and would let a typo
     * switch the rewrite off silently. An unrecognised value therefore leaves the rewrite on, which
     * is the behaviour every other setup gets; the cost of that choice is that {@code =0} or {@code
     * =no} is ignored without saying so, and ADR-0132 records it. A blank or valueless {@code -D}
     * counts as unset.
     */
    static boolean isDisabled(String property) {
        return property != null && "false".equalsIgnoreCase(property.strip());
    }

    /**
     * The wiring, with the Docker host passed in.
     *
     * <p>Package-private and taking the host rather than reading it, so a test can assert it
     * without starting Docker discovery — the public overload's {@code dockerHostIpAddress()}
     * resolves testcontainers' provider strategy, which a unit test must not do.
     *
     * <p><b>Every decision this class makes is reachable from a test</b>, which is not free: a
     * decision no test can drive can be deleted with the whole reactor still green, because a
     * wildcard publish is what testcontainers does unaided and nothing fails when the rewrite
     * simply does not happen. The loopback guard lives here for that reason and the opt-out lives
     * in the public overload for it — each left a surviving mutant when it sat anywhere else.
     *
     * <p>Returns the command it was given. Testcontainers 1.21.4 discards it — {@code
     * applyConfiguration} is {@code void} and {@code tryStart} keeps its own reference — so the
     * hazard is the inverse of the obvious one: a modifier that returned a <em>new</em> command
     * would have its changes silently dropped, which is why this rewrites in place.
     */
    static CreateContainerCmd modify(CreateContainerCmd createContainerCmd, String dockerHost) {
        String bindAddress = loopbackBindAddress(dockerHost);
        if (bindAddress != null) {
            HostConfig hostConfig = createContainerCmd.getHostConfig();
            if (hostConfig != null) {
                bindTo(hostConfig.getPortBindings(), bindAddress);
            }
        }
        return createContainerCmd;
    }

    /**
     * Rewrites every binding in {@code portBindings} to name {@code hostIp}, in place.
     *
     * <p>The host port spec is carried over rather than replaced, so a fixed host port stays fixed
     * and an unset one stays unset for Docker to assign. An exposed port with no binding at all
     * keeps none: adding one here would publish a port the container never asked to publish.
     *
     * <p>Separate from {@link #modify(CreateContainerCmd, String)} because {@code
     * CreateContainerCmd} has no implementation on this classpath outside testcontainers' own
     * relocated copy of docker-java, which checkstyle forbids naming — so the rewrite is asserted
     * on a {@link Ports} a test can build, and the wiring on a proxy.
     */
    static void bindTo(Ports portBindings, String hostIp) {
        if (portBindings == null) {
            return;
        }
        for (Map.Entry<ExposedPort, Ports.Binding[]> entry :
                portBindings.getBindings().entrySet()) {
            Ports.Binding[] bindings = entry.getValue();
            if (bindings == null) {
                continue;
            }
            for (int i = 0; i < bindings.length; i++) {
                if (bindings[i] != null) {
                    bindings[i] = new Ports.Binding(hostIp, bindings[i].getHostPortSpec());
                }
            }
        }
    }

    /**
     * The address to publish on when {@code dockerHost} — testcontainers' Docker host — is one this
     * JVM reaches over IPv4 loopback, or {@code null} when it is not.
     *
     * <p>It answers the <em>resolved</em> address rather than a constant, because loopback is the
     * whole {@code 127.0.0.0/8} block and not one address. Ubuntu ships {@code 127.0.1.1
     * <hostname>} in {@code /etc/hosts}, and {@code TESTCONTAINERS_HOST_OVERRIDE} is handed back
     * verbatim — so publishing a hard-coded {@code 127.0.0.1} for a host that resolves to {@code
     * 127.0.1.1} would bind where no client then looks and refuse every connection a harness makes.
     * What a published port must satisfy is the address a client in <em>this</em> JVM will connect
     * to, which is why the resolution is the JVM's own {@link InetAddress#getByName(String)}.
     *
     * <p>The blank guard is load-bearing rather than defensive tidiness: {@code getByName(null)}
     * and {@code getByName("")} both answer {@code 127.0.0.1}, so without it an <em>unset</em>
     * Docker host would classify as loopback and switch the rewrite on. A host that does not
     * resolve stands down instead, and so does an IPv6 loopback — {@code
     * java.net.preferIPv6Addresses=true}, or a host configured as {@code ::1} — because publishing
     * on IPv4 puts nothing at {@code ::1}. The wildcard publish is what testcontainers does
     * unaided, so it is the safe answer whenever this cannot tell.
     */
    static String loopbackBindAddress(String dockerHost) {
        if (dockerHost == null || dockerHost.isBlank()) {
            return null;
        }
        try {
            InetAddress address = InetAddress.getByName(dockerHost);
            return address.isLoopbackAddress() && address instanceof Inet4Address
                    ? address.getHostAddress()
                    : null;
        } catch (UnknownHostException e) {
            return null;
        }
    }
}
