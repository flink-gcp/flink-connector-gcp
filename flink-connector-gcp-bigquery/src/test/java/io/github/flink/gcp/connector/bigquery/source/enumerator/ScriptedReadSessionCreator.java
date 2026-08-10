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

package io.github.flink.gcp.connector.bigquery.source.enumerator;

import com.google.cloud.bigquery.storage.v1.AvroSchema;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.protobuf.Timestamp;
import io.github.flink.gcp.connector.bigquery.source.TestRows;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** A {@link ReadSessionCreator} answering with a canned session, and counting the calls. */
public final class ScriptedReadSessionCreator implements ReadSessionCreator {

    private static final long serialVersionUID = 1L;

    public static final String SESSION = "projects/p/locations/l/sessions/s";

    private final int streamCount;
    @Nullable private final RuntimeException failure;

    private final AtomicInteger creations = new AtomicInteger();
    private final AtomicInteger closes = new AtomicInteger();
    private final AtomicReference<CreateReadSessionRequest> lastRequest = new AtomicReference<>();

    /** The expiry the canned session carries, and therefore the one its splits do. */
    public static final Instant EXPIRE_TIME = Instant.parse("2026-08-09T12:00:00Z");

    private ScriptedReadSessionCreator(int streamCount, @Nullable RuntimeException failure) {
        this.streamCount = streamCount;
        this.failure = failure;
    }

    /** A creator answering with a session of the given stream count. */
    public static ScriptedReadSessionCreator withStreams(int streamCount) {
        return new ScriptedReadSessionCreator(streamCount, null);
    }

    /** A creator that fails every call. */
    public static ScriptedReadSessionCreator failing(RuntimeException failure) {
        return new ScriptedReadSessionCreator(0, failure);
    }

    /** Returns the name of the stream at the given index of the canned session. */
    public static String streamName(int index) {
        return SESSION + "/streams/" + index;
    }

    @Override
    public ReadSession create(CreateReadSessionRequest request) throws IOException {
        creations.incrementAndGet();
        lastRequest.set(request);
        if (failure != null) {
            throw failure;
        }
        ReadSession.Builder session =
                ReadSession.newBuilder()
                        .setName(SESSION)
                        .setAvroSchema(AvroSchema.newBuilder().setSchema(TestRows.SCHEMA_JSON))
                        .setExpireTime(
                                Timestamp.newBuilder().setSeconds(EXPIRE_TIME.getEpochSecond()));
        for (int i = 0; i < streamCount; i++) {
            session.addStreams(ReadStream.newBuilder().setName(streamName(i)));
        }
        return session.build();
    }

    @Override
    public void close() {
        closes.incrementAndGet();
    }

    int creations() {
        return creations.get();
    }

    /** Returns the last request this creator was handed, or {@code null} if it was handed none. */
    @Nullable
    CreateReadSessionRequest lastRequest() {
        return lastRequest.get();
    }

    int closes() {
        return closes.get();
    }
}
