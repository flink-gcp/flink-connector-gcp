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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.writer;

import org.apache.flink.util.ExceptionUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * In-memory {@link StagingStorage} fake mirroring GCS semantics: an object only becomes visible
 * when its stream is closed.
 */
public final class InMemoryStagingStorage implements StagingStorage {

    private static final long serialVersionUID = 1L;

    private final Map<String, byte[]> objects = new LinkedHashMap<>();
    private final List<String> deleted = new ArrayList<>();

    /**
     * When set, every object's stream throws it on close — reaching the writer as a failure to
     * finalize a staged file. An abort never closes the stream, so it never reaches this. Typed
     * {@code Throwable} so a test can script an {@code Error}, which is thrown as itself; a checked
     * exception arrives wrapped in a {@code RuntimeException}.
     */
    Throwable closeFailure;

    /**
     * When set, {@link #close()} throws it — the storage's own teardown failing, which is a
     * different thing from {@link #closeFailure} above: that one belongs to a staged object's
     * stream.
     */
    private Throwable storageCloseFailure;

    private int closeCount;

    /**
     * The channel may be closed on the writer's staging uploader, so the object map is guarded; the
     * scripted failure fields are set before a writer runs.
     */
    @Override
    public WritableByteChannel createObject(String gcsUri, int chunkBytes) {
        return Channels.newChannel(
                new ByteArrayOutputStream() {
                    @Override
                    public void close() throws IOException {
                        super.close();
                        synchronized (objects) {
                            objects.put(gcsUri, toByteArray());
                        }
                        if (closeFailure != null) {
                            ExceptionUtils.rethrow(closeFailure);
                        }
                    }
                });
    }

    @Override
    public void deleteObjects(List<String> gcsUris) {
        for (String gcsUri : gcsUris) {
            objects.remove(gcsUri);
            deleted.add(gcsUri);
        }
    }

    @Override
    public void close() throws Exception {
        closeCount++;
        if (storageCloseFailure != null) {
            ExceptionUtils.rethrow(storageCloseFailure);
        }
    }

    /** Counts the closes rather than raising a flag, which could not tell one close from two. */
    public int getCloseCount() {
        return closeCount;
    }

    /** Makes {@link #close()} throw, so a caller's ordering of its closes can be asserted. */
    public void failOnClose(Throwable failure) {
        this.storageCloseFailure = failure;
    }

    Map<String, byte[]> getObjects() {
        synchronized (objects) {
            return new LinkedHashMap<>(objects);
        }
    }

    public List<String> getDeleted() {
        return deleted;
    }
}
