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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import org.apache.flink.util.ExceptionUtils;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Int64Value;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Recording {@link BufferedStreamService} fake with scriptable outcomes, shared by the writer and
 * committer tests.
 *
 * <p>Created streams are named {@code tablePath/streams/fake-N}. Appends succeed echoing the
 * requested offset unless {@link #appendResults} holds a scripted future; stream creations throw
 * the head of {@link #createFailures} when non-empty; appender opens throw the head of {@link
 * #openAppenderFailures}; flushes consume {@link #flushResults} entries (a {@code Long} is
 * returned, a {@code Throwable} is thrown) and echo the requested offset when empty.
 */
public class FakeBufferedStreamService implements BufferedStreamService {

    /** One recorded offset append. */
    public static final class AppendCall {
        public final String streamName;
        public final ProtoRows rows;
        public final long offset;

        AppendCall(String streamName, ProtoRows rows, long offset) {
            this.streamName = streamName;
            this.rows = rows;
            this.offset = offset;
        }
    }

    /** One recorded flush. */
    public static final class FlushCall {
        public final String streamName;
        public final long offset;

        FlushCall(String streamName, long offset) {
            this.streamName = streamName;
            this.offset = offset;
        }
    }

    public final List<String> createdStreams = new ArrayList<>();
    public final List<String> openedAppenders = new ArrayList<>();
    public final List<Descriptors.Descriptor> openedDescriptors = new ArrayList<>();
    public final List<String> closedAppenders = new ArrayList<>();
    public final List<AppendCall> appends = new ArrayList<>();
    public final List<FlushCall> flushes = new ArrayList<>();

    public final Deque<Exception> createFailures = new ArrayDeque<>();
    public final Deque<Exception> openAppenderFailures = new ArrayDeque<>();
    public final Deque<ApiFuture<AppendRowsResponse>> appendResults = new ArrayDeque<>();
    public final Deque<Object> flushResults = new ArrayDeque<>();

    public boolean closed;

    /**
     * When set, {@link #close()} throws it after recording the close. Typed {@code Throwable} so a
     * test can script an {@code Error}, which is thrown as itself.
     */
    public Throwable closeFailure;

    /**
     * When set, every appender this service handed out throws it on close. Separate from {@link
     * #closeFailure} because the appender is <em>first</em> in the writer's close list and the
     * service second, so only this one exercises a failure that has resources after it.
     */
    public Throwable appenderCloseFailure;

    private int streamSequence;

    /** Returns a successful append response acknowledging the given offset. */
    public static ApiFuture<AppendRowsResponse> success(long offset) {
        return ApiFutures.immediateFuture(
                AppendRowsResponse.newBuilder()
                        .setAppendResult(
                                AppendRowsResponse.AppendResult.newBuilder()
                                        .setOffset(Int64Value.of(offset)))
                        .build());
    }

    /** Returns an append future failed with the given error. */
    public static ApiFuture<AppendRowsResponse> failure(Throwable error) {
        return ApiFutures.immediateFailedFuture(error);
    }

    @Override
    public String createBufferedStream(TableDestination destination) throws IOException {
        if (!createFailures.isEmpty()) {
            throwScripted(createFailures.removeFirst());
        }
        String name = destination.toTablePath() + "/streams/fake-" + streamSequence++;
        createdStreams.add(name);
        return name;
    }

    @Override
    public OffsetRowAppender openAppender(String streamName, Descriptors.Descriptor rowDescriptor)
            throws IOException {
        if (!openAppenderFailures.isEmpty()) {
            throwScripted(openAppenderFailures.removeFirst());
        }
        openedAppenders.add(streamName);
        openedDescriptors.add(rowDescriptor);
        return new OffsetRowAppender() {
            @Override
            public ApiFuture<AppendRowsResponse> append(ProtoRows rows, long offset) {
                appends.add(new AppendCall(streamName, rows, offset));
                if (!appendResults.isEmpty()) {
                    return appendResults.removeFirst();
                }
                return success(offset);
            }

            @Override
            public void close() {
                closedAppenders.add(streamName);
                if (appenderCloseFailure != null) {
                    ExceptionUtils.rethrow(appenderCloseFailure);
                }
            }
        };
    }

    @Override
    public long flushRows(String streamName, long offset) throws IOException {
        flushes.add(new FlushCall(streamName, offset));
        if (!flushResults.isEmpty()) {
            Object result = flushResults.removeFirst();
            if (result instanceof Long) {
                return (Long) result;
            }
            throwScripted((Exception) result);
        }
        return offset;
    }

    @Override
    public void close() {
        closed = true;
        if (closeFailure != null) {
            ExceptionUtils.rethrow(closeFailure);
        }
    }

    /** Returns a factory handing out this instance. */
    public BufferedStreamServiceFactory asFactory() {
        return new Factory();
    }

    private static void throwScripted(Exception e) throws IOException {
        if (e instanceof IOException) {
            throw (IOException) e;
        }
        throw (RuntimeException) e;
    }

    /** The options the factory was last asked to create a service with; see {@link #asFactory}. */
    @Nullable public BufferedStreamOptions createdWith;

    /** Not actually serializable — for direct writer/committer construction in tests only. */
    private final class Factory implements BufferedStreamServiceFactory {
        private static final long serialVersionUID = 1L;

        @Override
        public BufferedStreamService create(String location, BufferedStreamOptions options) {
            createdWith = options;
            return FakeBufferedStreamService.this;
        }
    }
}
