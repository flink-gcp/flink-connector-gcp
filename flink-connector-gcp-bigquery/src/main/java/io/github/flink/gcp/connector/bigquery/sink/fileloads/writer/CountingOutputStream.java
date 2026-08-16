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

import org.apache.flink.annotation.Internal;

import java.io.IOException;
import java.io.OutputStream;

/** Plain byte-counting stream wrapper (kept local to avoid a Guava dependency). */
@Internal
final class CountingOutputStream extends OutputStream {

    private final OutputStream delegate;
    private long count;

    CountingOutputStream(OutputStream delegate) {
        this.delegate = delegate;
    }

    long getCount() {
        return count;
    }

    @Override
    public void write(int b) throws IOException {
        delegate.write(b);
        count++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        delegate.write(b, off, len);
        count += len;
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
