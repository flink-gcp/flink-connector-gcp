<!--
Copyright 2026 The flink-gcp authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# ADR-0132: Container ports are published on the loopback address

- Status: Accepted
- Date: 2026-08-22
- Issues: [#1021](https://github.com/flink-gcp/flink-connector-gcp/issues/1021),
  [#1003](https://github.com/flink-gcp/flink-connector-gcp/issues/1003)
- Modules: all (tests)
- Current behavior: `LoopbackPortPublisher` in `flink-connector-gcp-test-utils`, registered through
  `META-INF/services`

## Context

Docker publishes a container's ports on the wildcard address. That **coexists** with a process
already holding `127.0.0.1:<port>`, because the more specific bind wins — so a client resolving
`localhost` to the IPv4 loopback reaches the stranger while the container sits there healthy, its
in-container listen check passing and its host-side connect passing because something did accept it.
The JVM resolves that way by default: `InetAddress.getAllByName("localhost")` returns `127.0.0.1`
ahead of `::1` absent `java.net.preferIPv6Addresses`.

Two sightings on one developer machine on 2026-08-22, four hours apart:

- [#1003](https://github.com/flink-gcp/flink-connector-gcp/issues/1003) — an unrelated desktop
  application's loopback API answered `401 Unauthorized` to a `tables.insert` the BigQuery emulator
  has no code path to produce. Reproduced end to end while diagnosing it: `curl` read 200 from the
  container (it reached `::1`) and a Java client on the same URL read the squatter's `401`.
- A `just verify` that died with `Could not connect to Ryuk at localhost:49751` while Ryuk's own
  container log in the same output read `Started address=[::]:8080`; `lsof -nP -iTCP:49751
  -sTCP:LISTEN` named a local IDE process. The re-run was green.

The second is the one that decides the shape. **Ryuk is contacted before any harness runs**, so a
collision on its port fails a whole module's integration tests with a message naming no connector at
all — and no harness in this repository constructs Ryuk's container, so nothing a harness could be
taught reaches it.

[#1011](https://github.com/flink-gcp/flink-connector-gcp/pull/1011) had already closed #1003 by
making the BigQuery emulator identify itself before a test trusts its port. That detects the
collision, for one emulator's REST port.

## Decision

Every container this repository starts publishes its ports on the loopback address the Docker
host resolves to, rather than on the wildcard address, whenever that host is one this JVM reaches
over IPv4 loopback.

This is one `org.testcontainers.core.CreateContainerCmdModifier` in
`flink-connector-gcp-test-utils`, registered through `META-INF/services` — a single class plus one
resource file, and no harness changes at all.

- **It removes the failure mode rather than detecting it.** Asking for the loopback address without
  a port makes the kernel pick a port that is free *on that address*, so the collision has nowhere
  to form. Docker refuses the exact-address collision outright — `-p 127.0.0.1:<held>:9050` fails
  with `bind: address already in use` — and only the wildcard publish is silent about it.
- **The host port spec is carried over, not replaced**, so a fixed host port stays fixed and an
  unset one stays unset. An exposed port with no binding gains none: publishing a port the container
  never asked to publish would be a new failure mode, not a fix.
- **Only when the Docker host resolves to an IPv4 loopback address in this JVM.** Against a remote
  daemon (`DOCKER_HOST=tcp://…`, Testcontainers Cloud) a loopback address is the *daemon's*, which
  the test JVM cannot reach; and a JVM resolving the Docker host to `::1` would be handed an
  endpoint with nothing listening on it. In both cases the wildcard publish stands. The collision is
  a same-machine one, so a remote daemon does not have it to begin with.
- **The address published on is the one the Docker host resolved to**, not a hard-coded
  `127.0.0.1`. Loopback is the whole `127.0.0.0/8` block: Ubuntu ships `127.0.1.1 <hostname>` in
  `/etc/hosts`, and `TESTCONTAINERS_HOST_OVERRIDE` is returned verbatim by
  `DockerClientProviderStrategy`. Publishing `127.0.0.1` for a host that resolves to `127.0.1.1`
  would bind where no client then looks and refuse every connection a harness makes — a setup that
  worked before this change, because the wildcard publish covered all of 127/8.
- **The `META-INF/services` registration is the whole mechanism**, so a unit test asserts that
  `ServiceLoader` resolves it. Without the file the class is dead code and every container silently
  returns to the wildcard publish with nothing failing.
- **An explicit opt-out, because the guard is an address test and one topology defeats it.**
  `FLINK_GCP_TESTS_LOOPBACK_PUBLISH=false`, or `-Dflink.gcp.tests.loopback-publish=false`, restores
  testcontainers' own wildcard publish. Only the literal `false` disables, case-insensitively and
  after stripping — not `Boolean.parseBoolean`, which reads every other spelling as `false` and
  would let a typo switch the rewrite off silently. The accepted cost of that direction is that
  `=0` or `=no` is ignored *without saying so*; a warning was weighed and declined because this
  runs per container and the class has no logger, so the silence is documented here instead.
  The key is dotted lowercase with a hyphen, matching Flink's own configuration-key style and this
  repository's one other property, `test.excluded.groups`; the `flink.gcp.tests.` prefix is new
  and deliberate, to keep a test-only knob out of any connector's option namespace.

## Evidence

**The SPI reaches everything, read from testcontainers 1.21.4 sources.**
`GenericContainer.loadCreateContainerCmdCustomizers()` is a `ServiceLoader` over the interface,
evaluated **per container instance**; the modifier loop is the last thing in
`applyConfiguration()`, after `ContainerDef.applyTo()` has written the bindings.
`RyukContainer extends GenericContainer`, and so does the `PortForwardingContainer` sshd image. No
harness in this repository uses `withCreateContainerCmdModifier`, `withHostConfig`,
`FixedHostPortGenericContainer` or `withPublishAllPorts`.

**The rewrite is not the last word *within* that loop, and what makes it survive is a dependency
detail — so this is the tripwire for a testcontainers or docker-java bump.** SPI entries and
`withCreateContainerCmdModifier` entries share one `LinkedHashSet`: the SPI ones are inserted by the
field initializer during `super(...)`, a subclass appends in its constructor body, and insertion
order is iteration order. This modifier therefore runs **first** and per-container ones run after —
and `RyukContainer` is one of them, calling
`cmd.withHostConfig(cmd.getHostConfig().withAutoRemove(true)…)`. The rewrite survives that only
because docker-java 3.4.2's fluent `HostConfig` setters mutate and return `this` (bytecode:
`withAutoRemove` and `withPortBindings` are both `putfield` then `areturn`), so Ryuk hands back the
very object this modifier edited. If a bump makes `RyukContainer` build a fresh `HostConfig`, or
makes those setters copy-on-write, **Ryuk alone** returns to the wildcard publish — the one
container this decision exists for — and nothing in the suite notices, because a wildcard publish is
what testcontainers does unaided. The check is the `docker ps` arm below, re-run on the bump. It is
deliberately not a runtime assertion: a fail-closed arm here would turn a silent degradation into a
build that cannot run at all.

**A loopback publish cannot form the collision, and a wildcard one forms it silently.** Both halves
re-measured 2026-08-22 rather than carried over from #1021, on Docker 27.4.0 / Docker Desktop for
macOS:

- With a process holding `127.0.0.1:49266`, `docker run -p 127.0.0.1:49266:80` is **refused** —
  `Ports are not available: … listen tcp 127.0.0.1:49266: bind: address already in use`. The same
  container published as `-p 49266:80` **starts**, and `docker ps` reads `0.0.0.0:49266->80/tcp`.
  That second line is the collision forming, on the same held port, with no error anywhere.
- With sixteen loopback ports held, six containers published as `-p 127.0.0.1::9050` were assigned
  six distinct ports, none of them among the sixteen, and `curl` read the emulator's own
  `{"datasetReference":…}` back from one. The kernel picks a port free *on that address*, which is
  the mechanism this decision rests on.

**The change does what it says, measured with a control arm.** `CloudTasksDispatchITCase` run twice
on 2026-08-22, `docker ps` polled at 0.5 s throughout:

| arm | Ryuk | sshd forwarder | Cloud Tasks emulator | wildcard publishes |
| --- | --- | --- | --- | --- |
| services file present | `127.0.0.1:57288->8080` | `127.0.0.1:57290->22` | `127.0.0.1:57294->8123` | 0 |
| services file removed | `0.0.0.0:57435->8080` | `0.0.0.0:57438->22` | `0.0.0.0:57442->8123` | 34 |

Both arms passed all 4 tests. The second arm is why the first is evidence: same containers, same
test, same machine, and the registration is the only difference.

**The container→host direction is untouched.** `AbstractCloudTasksEmulatorITCase` is the one harness
that uses `Testcontainers.exposeHostPorts`, and it was the first thing #1021 asked to check. Its
receiver is a JDK `HttpServer` bound to `InetAddress.getLoopbackAddress()` on the host; testcontainers
reaches it by `requestRemotePortForwarding(…)`, an SSH reverse tunnel through the sshd container, so
that direction never touches a port publish. The IT above is that harness, and it passes in both
arms. (`CloudTasksExamplesEmulator`, the other name #1021 flagged, is a compile-only documentation
snippet that starts no container.)

## Alternatives declined

**Teaching each harness to publish on the loopback.** It cannot reach Ryuk or the sshd forwarder,
which are exactly the containers whose failure names no connector — and it would mean touching every
container construction and remembering it at the next one. The SPI's per-instance loading is what
makes one registration reach containers this repository does not construct.

**Leaving #1011's probe as the whole answer.** A probe reports the collision; it does not prevent
it. It covers one emulator's REST port — that same class's javadoc records that its gRPC port has no
equivalent — and the Ryuk sighting is not reachable by any probe a harness could carry. The probe
stays regardless: it also catches a container that is merely unhealthy, and asking the emulator to
identify itself is a stronger statement than asking whether a socket answers.

**Binding `::1` as well.** Measured 2026-08-22: `docker run -p 127.0.0.1::9050 -p '[::1]::9050'`
gives one container port *two* host ports —
`{"9050/tcp":[{"HostIp":"::1","HostPort":"49694"},{"HostIp":"127.0.0.1","HostPort":"49693"}]}`.
`getMappedPort` reports `binding[0]`, so the endpoint a test is handed would depend on map ordering.
An ambiguous endpoint is worse than the residual IPv6 hazard under Consequences, which only a
hand-run `curl` can meet. (This was declined originally to protect `curl localhost`, which turned
out not to need protecting — see Consequences. The measurement above is why it stays declined.)

**A per-developer `~/.testcontainers.properties` setting.** Not tracked, so it protects whoever
knows to set it and no one else, and CI would differ from every developer machine for no stated
reason.

**Doing nothing, on the grounds that CI is unaffected.** True and not sufficient: a fresh
`ubuntu-latest` runner is unlikely to have a loopback squatter, so the benefit is developer-machine
reliability. Two sightings in four hours on one machine, one of them costing a whole module's
integration tests and a re-run, is the argument. It is why #1021 is P2 rather than higher: nothing
on a shipped path breaks if it waits.

## Consequences

- **The ergonomic cost #1021 expected does not exist.** The issue predicted that `curl
  http://localhost:<port>` would fail, because `curl` prefers `::1` and nothing is bound there any
  more, and called it "a real cost to pay knowingly". Measured 2026-08-22 with curl 8.7.1 on macOS
  against a loopback-published container: `Trying [::1]…` → `connect to ::1 … failed: Connection
  refused` → `Trying 127.0.0.1…` → `Connected`. The refusal is immediate, so curl falls back and the
  request succeeds. **The premise was false and the decision does not rest on it.**
- **What is left is smaller and points the other way.** That fallback rests on the refusal being
  instant, and if something *is* listening on `::1:<port>` curl reaches it rather than the
  container — the same shadowing hazard as #1003, moved to IPv6, and now reachable only by a
  hand-run `curl`: the JVM resolves `127.0.0.1` first and meets a specific bind there. So
  `curl http://127.0.0.1:<port>` is still the habit worth keeping, for unambiguity rather than
  because `localhost` fails. The build-traps entry in `.agents/references/repository-guide.md` says
  exactly that.
- The `curl`-says-fine/test-says-not diagnosis that #1003 and the Ryuk sighting both showed still
  applies wherever this modifier stands down — a remote daemon, an IPv6-resolving JVM — so the
  diagnosis stays recorded rather than being deleted as solved.
- `flink-connector-gcp-test-utils` gains a `src/main/resources` tree it did not have, and with it
  its **first behavior that activates by classpath presence rather than by a harness calling
  something**. That is an input to the publishing decision the module's pom defers to #29/#39: if
  test-utils is ever published, a downstream consumer putting it on a test classpath gets every one
  of *their* containers rebound without importing anything — reachable only through the opt-out
  above, which they would have to know exists. Nothing is published today.
- `BigQueryEmulatorContainers`' port-shadowing paragraph now records what the probe defends against
  rather than an open hazard.
- **The guard is an address test, and one topology defeats it** — raised by the independent review
  round. A daemon reached *at* a loopback address but running elsewhere (an SSH tunnel on
  `tcp://127.0.0.1:2375`; a VM whose port forwarder carries only wildcard-bound guest ports, as
  colima and Rancher Desktop may) looks exactly like a local daemon to
  `dockerHostIpAddress()`. There the rewrite engages, binds inside the daemon, and the connection is
  refused — loudly, and in test infrastructure only. It is **not** inferred away, because a local
  daemon deliberately exposed over `tcp://127.0.0.1` is a setup this should serve, and a
  scheme-based guard would disable that one to rescue the other. Hence the opt-out above. Docker
  Desktop for macOS is itself VM-backed and is measured working without it; colima and a tunnelled
  daemon are **not measured here**, which is why the escape is documented rather than the topology
  being claimed either way.
