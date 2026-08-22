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

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Ports;
import org.junit.jupiter.api.Test;
import org.testcontainers.core.CreateContainerCmdModifier;

import java.lang.reflect.Proxy;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Covers what no consumer of this module can reach: the port rewrite, the wiring that applies it,
 * and the {@code META-INF/services} registration that is the only reason it is applied at all.
 *
 * <p>The wiring needs asserting here precisely because its absence is invisible everywhere else —
 * inverting the guard or dropping the rewrite call leaves every test in the reactor green, since a
 * wildcard publish is what testcontainers does unaided.
 *
 * <p>Nothing here constructs a real {@code CreateContainerCmd}. Its only implementation on this
 * classpath comes from testcontainers' own relocated copy of docker-java, which checkstyle forbids
 * naming and which would break this test on a testcontainers upgrade for reasons unrelated to what
 * it asserts. The interface itself is unrelocated, so a proxy answers instead.
 */
class LoopbackPortPublisherTest {

    private static final ExposedPort PORT = ExposedPort.tcp(9050);

    @Test
    void aDynamicBindingIsMovedToTheGivenAddressAndStaysDynamic() {
        Ports bindings = new Ports(PORT, Ports.Binding.empty());

        LoopbackPortPublisher.bindTo(bindings, "127.0.0.1");

        // The port spec stays unset, which is what leaves Docker to pick one — and, because the
        // address is now specific, to pick one that is free on that address.
        assertThat(bindings.getBindings().get(PORT))
                .containsExactly(new Ports.Binding("127.0.0.1", null));
    }

    @Test
    void aFixedHostPortSurvivesTheRewrite() {
        Ports bindings = new Ports(PORT, Ports.Binding.bindPort(18050));

        LoopbackPortPublisher.bindTo(bindings, "127.0.0.1");

        assertThat(bindings.getBindings().get(PORT))
                .containsExactly(new Ports.Binding("127.0.0.1", "18050"));
    }

    @Test
    void everyBindingOfEveryPortIsRewritten() {
        ExposedPort grpc = ExposedPort.tcp(9060);
        Ports bindings = new Ports(PORT, Ports.Binding.empty());
        bindings.bind(PORT, Ports.Binding.bindIpAndPort("0.0.0.0", 18050));
        bindings.bind(grpc, Ports.Binding.empty());

        LoopbackPortPublisher.bindTo(bindings, "127.0.0.1");

        // The second binding already named an address, and a wildcard one at that: rewriting only
        // the unset ones would leave exactly the publish this exists to prevent.
        assertThat(bindings.getBindings().get(PORT))
                .containsExactly(
                        new Ports.Binding("127.0.0.1", null),
                        new Ports.Binding("127.0.0.1", "18050"));
        assertThat(bindings.getBindings().get(grpc))
                .containsExactly(new Ports.Binding("127.0.0.1", null));
    }

    @Test
    void anExposedPortWithNoBindingGainsNone() {
        Ports bindings = new Ports();
        bindings.bind(PORT, null);

        LoopbackPortPublisher.bindTo(bindings, "127.0.0.1");

        // Publishing a port the container never asked to publish would be a new failure mode, not
        // a fix: the container's ports are what its harness declared.
        assertThat(bindings.getBindings()).containsOnlyKeys(PORT).containsEntry(PORT, null);
    }

    @Test
    void aLoopbackDockerHostRewritesTheCommandsBindings() {
        Ports bindings = new Ports(PORT, Ports.Binding.empty());
        CreateContainerCmd command = commandWith(new HostConfig().withPortBindings(bindings));

        CreateContainerCmd returned = LoopbackPortPublisher.modify(command, "127.0.0.1");

        assertThat(bindings.getBindings().get(PORT))
                .containsExactly(new Ports.Binding("127.0.0.1", null));
        // Testcontainers discards this return value, so the assertion pins the inverse hazard:
        // a modifier answering a *new* command would have its changes silently dropped.
        assertThat(returned).isSameAs(command);
    }

    @Test
    void aRemoteDockerHostLeavesTheCommandsBindingsAlone() {
        Ports bindings = new Ports(PORT, Ports.Binding.empty());
        CreateContainerCmd command = commandWith(new HostConfig().withPortBindings(bindings));

        CreateContainerCmd returned = LoopbackPortPublisher.modify(command, "192.0.2.1");

        assertThat(bindings.getBindings().get(PORT)).containsExactly(Ports.Binding.empty());
        assertThat(returned).isSameAs(command);
    }

    @Test
    void theAddressPublishedOnIsTheOneTheDockerHostResolvesTo() {
        // Loopback is the whole 127/8 block, not one address: Ubuntu ships `127.0.1.1 <hostname>`
        // in /etc/hosts, and TESTCONTAINERS_HOST_OVERRIDE is handed back verbatim. Publishing a
        // hard-coded 127.0.0.1 for such a host would bind where no client then looks.
        assertThat(LoopbackPortPublisher.loopbackBindAddress("127.0.1.1")).isEqualTo("127.0.1.1");
        assertThat(LoopbackPortPublisher.loopbackBindAddress("127.0.0.2")).isEqualTo("127.0.0.2");
    }

    @Test
    void aLocalDockerSocketsLocalhostGivesTheIpv4Loopback() throws UnknownHostException {
        // `localhost` is the value testcontainers reports for a local Docker socket, so it is the
        // real production input and worth pinning. What it resolves to, though, is the JVM's
        // choice — and a JVM answering `::1` is a configuration this class deliberately stands
        // down on, so asserting `127.0.0.1` unconditionally would turn a *supported* setup into a
        // red `just verify`. The assumption states the environment; the assertion still pins the
        // behaviour in it. Every other input in this class is a literal and needs no such guard.
        assumeTrue(
                InetAddress.getByName("localhost") instanceof Inet4Address,
                "this JVM resolves localhost to the IPv6 loopback, where standing down is correct");

        assertThat(LoopbackPortPublisher.loopbackBindAddress("localhost")).isEqualTo("127.0.0.1");
    }

    @Test
    void anIpv6LoopbackDockerHostStandsDown() {
        // Publishing on IPv4 puts nothing at ::1, so a JVM that resolves the Docker host to the
        // IPv6 loopback would be handed an endpoint it cannot reach. Doing nothing is correct.
        assertThat(LoopbackPortPublisher.loopbackBindAddress("::1")).isNull();
    }

    @Test
    void aRemoteDockerHostStandsDown() {
        // A remote daemon's loopback is its own, not this JVM's. RFC 5737 TEST-NET-1, which is
        // documentation-only and routes nowhere.
        assertThat(LoopbackPortPublisher.loopbackBindAddress("192.0.2.1")).isNull();
    }

    @Test
    void aDockerHostThatDoesNotResolveStandsDown() {
        // RFC 2606 reserves .invalid precisely so this never depends on a resolver's mood. Without
        // this the UnknownHostException arm is unasserted, and answering the loopback address
        // there would engage the rewrite on a host nothing can reach.
        assertThat(LoopbackPortPublisher.loopbackBindAddress("no-such-docker-host.invalid"))
                .isNull();
    }

    @Test
    void anUnsetDockerHostStandsDown() {
        // The guard this pins is load-bearing, not tidiness: InetAddress.getByName(null) and
        // getByName("") both answer 127.0.0.1, so dropping it would make an unset Docker host
        // classify as loopback and switch the rewrite on.
        assertThat(LoopbackPortPublisher.loopbackBindAddress(null)).isNull();
        assertThat(LoopbackPortPublisher.loopbackBindAddress("")).isNull();
        assertThat(LoopbackPortPublisher.loopbackBindAddress("  ")).isNull();
    }

    @Test
    void anExplicitOptOutStopsTheRewriteThroughThePublicEntryPoint() {
        Ports bindings = new Ports(PORT, Ports.Binding.empty());
        CreateContainerCmd command = commandWith(new HostConfig().withPortBindings(bindings));

        // Driving the SPI method itself, not the seam: this is what pins the constant's spelling
        // and the order the two sources are read in. It is only reachable because the opt-out is
        // checked before the Docker host is resolved — otherwise this would start Docker
        // discovery. With the property set, nothing here touches testcontainers.
        System.setProperty(LoopbackPortPublisher.LOOPBACK_PUBLISH_PROPERTY, "false");
        try {
            CreateContainerCmd returned = new LoopbackPortPublisher().modify(command);

            assertThat(bindings.getBindings().get(PORT)).containsExactly(Ports.Binding.empty());
            assertThat(returned).isSameAs(command);
        } finally {
            System.clearProperty(LoopbackPortPublisher.LOOPBACK_PUBLISH_PROPERTY);
        }
    }

    @Test
    void theOptOutNamesTheRepositoryDocumentsAreTheOnesTheCodeReads() {
        // ADR-0132, this class's javadoc and the repository guide all quote these two spellings,
        // and a typo in either would leave all other tests green while the escape hatch is dead
        // for the one user it exists for.
        assertThat(LoopbackPortPublisher.LOOPBACK_PUBLISH_PROPERTY)
                .isEqualTo("flink.gcp.tests.loopback-publish");
        assertThat(LoopbackPortPublisher.LOOPBACK_PUBLISH_ENV)
                .isEqualTo("FLINK_GCP_TESTS_LOOPBACK_PUBLISH");
    }

    @Test
    void onlyTheValueFalseDisables() {
        assertThat(LoopbackPortPublisher.isDisabled("false", null)).isTrue();
        assertThat(LoopbackPortPublisher.isDisabled(null, "false")).isTrue();
        assertThat(LoopbackPortPublisher.isDisabled(null, "FALSE")).isTrue();
        // A CI `env:` block readily produces trailing whitespace.
        assertThat(LoopbackPortPublisher.isDisabled(null, " false ")).isTrue();
        // Not Boolean.parseBoolean, which reads every non-"true" spelling as false: a typo must
        // not switch the rewrite off silently. The cost is that these are ignored in silence.
        assertThat(LoopbackPortPublisher.isDisabled("no", null)).isFalse();
        assertThat(LoopbackPortPublisher.isDisabled("0", null)).isFalse();
        assertThat(LoopbackPortPublisher.isDisabled(null, null)).isFalse();
    }

    @Test
    void aSetPropertyWinsOverTheEnvironmentButAValuelessOneDoesNot() {
        assertThat(LoopbackPortPublisher.isDisabled("true", "false")).isFalse();
        // `-Dflink.gcp.tests.loopback-publish` with no value answers "", which must not mask an
        // environment variable the developer did set.
        assertThat(LoopbackPortPublisher.isDisabled("", "false")).isTrue();
    }

    @Test
    void theServicesFileRegistersThisModifier() {
        // The registration is the whole mechanism: without this file the class above is dead code
        // and every container goes back to the wildcard publish, silently and with nothing failing.
        // One-arg load(), like GenericContainer.loadCreateContainerCmdCustomizers, so this resolves
        // the way production does.
        assertThat(ServiceLoader.load(CreateContainerCmdModifier.class))
                .hasAtLeastOneElementOfType(LoopbackPortPublisher.class);
    }

    /**
     * A {@link CreateContainerCmd} that answers {@code getHostConfig} and refuses everything else,
     * so a test of the wiring cannot silently depend on any other part of the command.
     */
    private static CreateContainerCmd commandWith(HostConfig hostConfig) {
        return (CreateContainerCmd)
                Proxy.newProxyInstance(
                        CreateContainerCmd.class.getClassLoader(),
                        new Class<?>[] {CreateContainerCmd.class},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "getHostConfig":
                                    return hostConfig;
                                // AssertJ's isSameAs reports on failure, which needs these.
                                case "hashCode":
                                    return System.identityHashCode(proxy);
                                case "equals":
                                    return proxy == args[0];
                                case "toString":
                                    return "CreateContainerCmd(proxy)";
                                default:
                                    throw new UnsupportedOperationException(method.getName());
                            }
                        });
    }
}
