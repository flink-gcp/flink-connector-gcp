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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Creates synthetic service-account keys for credential wiring tests: structurally valid enough for
 * the Google auth libraries to parse — a real RSA key in a real PEM envelope — while naming an
 * {@code example.invalid} account no service would ever accept, so a test that leaks one at a real
 * endpoint fails loudly rather than authenticating.
 */
@Internal
public final class ServiceAccountKeyFiles {

    public static final String CLIENT_EMAIL = "service-account@example.invalid";

    private ServiceAccountKeyFiles() {}

    /**
     * Writes a valid synthetic service-account key below {@code directory}, always as {@code
     * service-account.json} — a second call with the same directory overwrites the first key.
     */
    public static Path create(Path directory) throws IOException {
        Path keyFile = directory.resolve("service-account.json");
        Files.writeString(keyFile, json(), StandardCharsets.UTF_8);
        return keyFile;
    }

    /**
     * Returns the key as the JSON document itself, for a test that feeds the material somewhere
     * other than a file of this class's choosing.
     */
    public static String json() {
        return json("test-key-id");
    }

    /**
     * Returns the key with the given {@code private_key_id}, for a test that plants a sentinel
     * there and asserts it never surfaces elsewhere — a serialized form, a log line.
     */
    public static String json(String privateKeyId) {
        // The id is spliced into the document verbatim, so a character JSON would escape — a
        // quote, a backslash, or a control character — turns the key into a parse error a caller
        // meets far away, inside the auth library. A plain throw rather than Flink's
        // Preconditions, which is @Internal and would need a tier-audit allowlist entry for one
        // call site (the ShadedJar.of shape).
        if (privateKeyId.contains("\"")
                || privateKeyId.contains("\\")
                || privateKeyId.chars().anyMatch(c -> c < 0x20)) {
            throw new IllegalArgumentException(
                    "The private_key_id is spliced into the key JSON unescaped and must not"
                            + " contain '\"', '\\' or control characters: "
                            + privateKeyId);
        }
        KeyPairGenerator generator = newRsaGenerator();
        generator.initialize(2048);
        KeyPair keyPair = generator.generateKeyPair();
        String encoded =
                Base64.getMimeEncoder(64, new byte[] {'\n'})
                        .encodeToString(keyPair.getPrivate().getEncoded());
        String privateKey =
                "-----BEGIN PRIVATE KEY-----\n" + encoded + "\n-----END PRIVATE KEY-----\n";
        return "{"
                + "\"type\":\"service_account\","
                + "\"project_id\":\"test-project\","
                + "\"private_key_id\":\""
                + privateKeyId
                + "\","
                + "\"private_key\":\""
                + privateKey.replace("\n", "\\n")
                + "\","
                + "\"client_email\":\""
                + CLIENT_EMAIL
                + "\","
                + "\"client_id\":\"1234567890\","
                + "\"auth_uri\":\"https://accounts.google.com/o/oauth2/auth\","
                + "\"token_uri\":\"https://oauth2.googleapis.com/token\""
                + "}";
    }

    /** RSA is a {@link KeyPairGenerator} algorithm every JDK must provide, so this cannot fail. */
    private static KeyPairGenerator newRsaGenerator() {
        try {
            return KeyPairGenerator.getInstance("RSA");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("The JDK is required to provide RSA.", e);
        }
    }
}
