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

package io.github.flink.gcp.connector.bigtable.sink.readmodifywrite;

import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import io.github.flink.gcp.connector.bigtable.TableDestination;

import java.util.List;

/**
 * The rules a request puts on the wire, for tests outside this package: a {@link
 * ReadModifyWriteRule} model exposes no accessors, so its content is observable only through the
 * request the runtime builds from it.
 */
public final class ReadModifyWriteWireRules {
    private ReadModifyWriteWireRules() {}

    /**
     * Builds the request against a fake client and returns its rules in wire order.
     *
     * @param request the request to build
     * @return the protobuf rules it sends
     */
    public static List<com.google.bigtable.v2.ReadModifyWriteRule> sentRules(
            ReadModifyWriteRequest request) {
        FakeReadModifyWriteClients client = new FakeReadModifyWriteClients();
        request.toRequest().start(client, TableDestination.of("p", "i", "t"));
        return client.sent.get(0).toProto(RequestContext.create("p", "i", "")).getRulesList();
    }
}
