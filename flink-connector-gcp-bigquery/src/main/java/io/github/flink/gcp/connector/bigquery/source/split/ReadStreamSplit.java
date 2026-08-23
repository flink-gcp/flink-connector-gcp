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

package io.github.flink.gcp.connector.bigquery.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.Objects;

/**
 * One Storage Read API {@code ReadStream}, plus how far into it the source has already consumed.
 *
 * <p>The offset counts input rows successfully processed, which is what {@code
 * ReadRowsRequest.offset} takes, so a restored split resumes exactly where the last checkpoint left
 * off. It is advanced by the record emitter once after each successful deserializer call and its
 * synchronous downstream emissions. This class is the immutable form that is checkpointed and
 * assigned, while {@link ReadStreamSplitState} is the mutable form a reader works with.
 *
 * <p>The split carries the session's Avro schema because a reader is handed splits and nothing
 * else: both {@code ReadSession} and the responses of a {@code ReadRows} call do report the schema,
 * but the latter is not something the API contract promises for every call, and a resumed read that
 * arrives without one has no other source for it.
 *
 * <p>It carries the session's expiry for the same reason, and for one purpose: a read that fails
 * after the session has expired is explained rather than left as a bare stream error. It is not a
 * deadline this connector enforces — the expiry is BigQuery's to apply, and a client clock is not
 * the one that decides — so nothing here refuses to open a stream because of it. A split restored
 * from a checkpoint written before the field existed carries {@code null}, and the failure is
 * reported unannotated.
 *
 * <p>A checkpoint can be taken between the last row of a stream being emitted and the reader
 * recording the stream as finished, so a restored split can sit at exactly the stream's row count.
 * Nothing marks it: BigQuery answers such a read with an empty stream and no error (measured
 * 2026-08-09), which the reader reports as a finished split at the cost of one empty call. A {@code
 * finished} flag was written and removed — no code path could set it, because {@code
 * SourceReaderBase} removes a split's state before telling the reader the split finished.
 */
@Internal
public final class ReadStreamSplit implements SourceSplit {

    private final String streamName;
    private final long offset;
    private final String avroSchemaJson;
    @Nullable private final Instant sessionExpireTime;

    /**
     * Creates a split.
     *
     * @param streamName the {@code projects/../locations/../sessions/../streams/..} stream name
     * @param offset how many input rows of this stream have already been consumed
     * @param avroSchemaJson the read session's Avro schema, in its JSON form
     * @param sessionExpireTime when the read session expires, or {@code null} when it is not known
     */
    public ReadStreamSplit(
            String streamName,
            long offset,
            String avroSchemaJson,
            @Nullable Instant sessionExpireTime) {
        Preconditions.checkArgument(offset >= 0, "offset must not be negative: %s", offset);
        this.streamName = Preconditions.checkNotNull(streamName, "streamName must not be null");
        this.offset = offset;
        this.avroSchemaJson =
                Preconditions.checkNotNull(avroSchemaJson, "avroSchemaJson must not be null");
        this.sessionExpireTime = sessionExpireTime;
    }

    /** Returns the stream's resource name, which is also this split's id. */
    public String getStreamName() {
        return streamName;
    }

    /** Returns how many input rows of this stream have already been consumed. */
    public long getOffset() {
        return offset;
    }

    /** Returns the read session's Avro schema, in its JSON form. */
    public String getAvroSchemaJson() {
        return avroSchemaJson;
    }

    /** Returns when the read session expires, or {@code null} when it is not known. */
    @Nullable
    public Instant getSessionExpireTime() {
        return sessionExpireTime;
    }

    @Override
    public String splitId() {
        return streamName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ReadStreamSplit)) {
            return false;
        }
        ReadStreamSplit other = (ReadStreamSplit) o;
        return offset == other.offset
                && streamName.equals(other.streamName)
                && avroSchemaJson.equals(other.avroSchemaJson)
                && Objects.equals(sessionExpireTime, other.sessionExpireTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamName, offset, avroSchemaJson, sessionExpireTime);
    }

    @Override
    public String toString() {
        // The schema is deliberately left out: it is kilobytes of JSON and every log line carrying
        // a split would otherwise carry the whole table's shape. The expiry is a single instant and
        // is the thing an assignment log line is read for when a long read starts failing.
        return "ReadStreamSplit{streamName='"
                + streamName
                + "', offset="
                + offset
                + ", sessionExpireTime="
                + sessionExpireTime
                + '}';
    }
}
