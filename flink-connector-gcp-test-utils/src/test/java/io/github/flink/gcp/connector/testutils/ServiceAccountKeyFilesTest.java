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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The one rejection {@link ServiceAccountKeyFiles#json(String)} owns.
 *
 * <p>Tested here rather than through a consumer, by this module's narrow bar: every consumer passes
 * a key id this accepts, so no green build reaches the branch. Without the rejection, a sentinel
 * carrying a JSON-significant character would corrupt the document and surface as a parse error
 * inside the auth library, far from the call that caused it. Everything else about the type — that
 * the key parses as a service account, the scopes it earns, the sentinel round trip — is exercised
 * for real by the credential tests in all five connector modules, and so is deliberately not
 * repeated here.
 */
class ServiceAccountKeyFilesTest {

    @ParameterizedTest
    @ValueSource(strings = {"key\"id", "key\\id", "key\nid"})
    void aKeyIdCarryingAJsonSignificantCharacterIsRejected(String privateKeyId) {
        // All three shapes, because the check is about corrupting the document and each one
        // alone does it: a check written for the quote only would admit the backslash, and one
        // written for both would still admit a control character.
        assertThatThrownBy(() -> ServiceAccountKeyFiles.json(privateKeyId))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unescaped");
    }
}
