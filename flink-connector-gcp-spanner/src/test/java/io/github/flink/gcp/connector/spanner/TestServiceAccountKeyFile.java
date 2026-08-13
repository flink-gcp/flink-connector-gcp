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

package io.github.flink.gcp.connector.spanner;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Base64;

/** Creates a syntactically valid service-account key for credential plumbing tests. */
public final class TestServiceAccountKeyFile {

    private TestServiceAccountKeyFile() {}

    public static Path create(Path directory) throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keyPair = generator.generateKeyPair();
        String encoded =
                Base64.getMimeEncoder(64, new byte[] {'\n'})
                        .encodeToString(keyPair.getPrivate().getEncoded());
        String privateKey =
                "-----BEGIN PRIVATE KEY-----\n" + encoded + "\n-----END PRIVATE KEY-----\n";
        String json =
                "{"
                        + "\"type\":\"service_account\","
                        + "\"project_id\":\"test-project\","
                        + "\"private_key_id\":\"test-key-id\","
                        + "\"private_key\":\""
                        + privateKey.replace("\n", "\\n")
                        + "\","
                        + "\"client_email\":\"service-account@example.invalid\","
                        + "\"client_id\":\"1234567890\","
                        + "\"auth_uri\":\"https://accounts.google.com/o/oauth2/auth\","
                        + "\"token_uri\":\"https://oauth2.googleapis.com/token\""
                        + "}";
        Path keyFile = directory.resolve("service-account.json");
        Files.writeString(keyFile, json, StandardCharsets.UTF_8);
        return keyFile;
    }
}
