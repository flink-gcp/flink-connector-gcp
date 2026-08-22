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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link ReadSessionCreator} answering with a canned session, and counting the calls.
 *
 * <p>Refusing after {@link #close()} mirrors {@link ReadClientSessionCreator}: without it a creator
 * shared between two enumerators behaves exactly like a fresh one, which is why nothing caught
 * issue #990. A test that hands one to a source goes through {@link Factory}, which is the
 * serializable half.
 */
public final class ScriptedReadSessionCreator implements ReadSessionCreator {

    public static final String SESSION = "projects/p/locations/l/sessions/s";

    private final int streamCount;
    @Nullable private final RuntimeException failure;

    private final AtomicInteger creations = new AtomicInteger();
    private final AtomicInteger closes = new AtomicInteger();
    private final AtomicReference<CreateReadSessionRequest> lastRequest = new AtomicReference<>();

    private volatile boolean closed;

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
        if (closed) {
            throw new IOException(
                    "The BigQuery read session creator was closed; the source is shutting down.");
        }
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
        closed = true;
        closes.incrementAndGet();
    }

    /** Returns whether this creator refuses further session creation. */
    public boolean isClosed() {
        return closed;
    }

    public int creations() {
        return creations.get();
    }

    /** Returns the last request this creator was handed, or {@code null} if it was handed none. */
    @Nullable
    CreateReadSessionRequest lastRequest() {
        return lastRequest.get();
    }

    public int closes() {
        return closes.get();
    }

    /**
     * Mints scripted creators, and is what a test hands to a source builder.
     *
     * <p>Serializable, as the seam on the configuration now is; the creators it mints are not, and
     * the list of them is {@code transient} so a copy deserialized inside a MiniCluster job records
     * its own rather than pretending to share the test's.
     */
    public static final class Factory implements ReadSessionCreatorFactory {

        private static final long serialVersionUID = 1L;

        private final int streamCount;

        /**
         * The seams minted here.
         *
         * <p>{@code transient} because a copy of this factory deserialized inside a MiniCluster job
         * records its own; concurrent because {@code create()} may run on a coordinator worker
         * thread while a test reads the list on its own.
         */
        @Nullable private transient volatile List<ScriptedReadSessionCreator> minted;

        private Factory(int streamCount) {
            this.streamCount = streamCount;
        }

        /** A factory minting creators answering with a session of the given stream count. */
        public static Factory withStreams(int streamCount) {
            return new Factory(streamCount);
        }

        @Override
        public ReadSessionCreator create() {
            ScriptedReadSessionCreator creator = new ScriptedReadSessionCreator(streamCount, null);
            recorded().add(creator);
            return creator;
        }

        /** Returns the creators minted here, in the order they were minted. */
        public List<ScriptedReadSessionCreator> minted() {
            return new ArrayList<>(recorded());
        }

        private synchronized List<ScriptedReadSessionCreator> recorded() {
            if (minted == null) {
                minted = new CopyOnWriteArrayList<>();
            }
            return minted;
        }

        /**
         * Returns the one creator minted, failing when there was not exactly one.
         *
         * <p>The count is half of what a caller asserts: one enumerator mints one creator, and a
         * source that minted two would otherwise pass every count assertion.
         */
        public ScriptedReadSessionCreator only() {
            if (recorded().size() != 1) {
                throw new AssertionError(
                        "expected exactly one minted creator but was " + recorded().size());
            }
            return recorded().get(0);
        }
    }
}
