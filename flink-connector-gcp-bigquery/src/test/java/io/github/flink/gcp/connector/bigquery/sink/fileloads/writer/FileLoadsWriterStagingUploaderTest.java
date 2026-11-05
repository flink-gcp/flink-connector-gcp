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

import org.apache.flink.api.connector.sink2.SinkWriter;

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsCommittable;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.FileLoadsOptions;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.StagingFormat;
import io.github.flink.gcp.connector.bigquery.sink.fileloads.writer.FileLoadsWriterTest.TestRow;
import io.github.flink.gcp.connector.testutils.LogCapture;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Tests for {@link FileLoadsWriter} with its background staging uploader enabled. */
class FileLoadsWriterStagingUploaderTest {

    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;
    private static final int CHUNK = 256 * 1024;
    private static final String UPLOADER = "FileLoadsWriter staging uploader ";

    @Test
    void sizeRolledFilesFinalizeOnTheUploaderAndCommitInCloseOrder() throws Exception {
        RecordingStorage storage = new RecordingStorage();
        FileLoadsWriter<TestRow> writer = writer(storage, uploaderOptions().maxStagingFileBytes(1));

        writer.write(new TestRow("t1", "a", 1L), CONTEXT);
        writer.write(new TestRow("t2", "b", 2L), CONTEXT);
        writer.write(new TestRow("t1", "c", 3L), CONTEXT);
        List<FileLoadsCommittable> committables = new ArrayList<>(writer.prepareCommit());
        writer.close();

        assertThat(committables).hasSize(3);
        assertThat(committables.stream().map(FileLoadsCommittable::getUri))
                .containsExactlyElementsOf(storage.closedUris());
        assertThat(storage.closeThreads())
                .allSatisfy(name -> assertThat(name).startsWith(UPLOADER));
        List<String> names = new ArrayList<>();
        for (FileLoadsCommittable committable : committables) {
            assertThat(committable.getRowCount()).isOne();
            names.addAll(rowNames(storage.object(committable.getUri())));
        }
        assertThat(names).containsExactly("a", "b", "c");
        assertThat(storage.chunkSizes()).containsOnly(CHUNK);
    }

    @Test
    void multiChunkFilesSendFullChunksOnTheUploaderAndStageTheSameRowsAsWithout() throws Exception {
        List<TestRow> rows = largeRows(6);

        RecordingStorage withUploader = new RecordingStorage();
        FileLoadsWriter<TestRow> uploaderWriter = writer(withUploader, uploaderOptions());
        for (TestRow row : rows) {
            uploaderWriter.write(row, CONTEXT);
        }
        // Taken before the checkpoint, whose finalization writes the last, partial chunk. With
        // two permits, all but the last two upload operations have already run, so this is not
        // empty.
        List<String> writesBeforeCheckpoint = withUploader.writes();
        List<String> uploaderRows = commitAndRead(uploaderWriter, withUploader);

        RecordingStorage withoutUploader = new RecordingStorage();
        FileLoadsWriter<TestRow> serialWriter =
                writer(
                        withoutUploader,
                        FileLoadsOptions.builder()
                                .stagingPath("gs://bucket/prefix")
                                .stagingUploadChunkBytes(CHUNK));
        for (TestRow row : rows) {
            serialWriter.write(row, CONTEXT);
        }
        List<String> serialRows = commitAndRead(serialWriter, withoutUploader);

        // Avro's sync marker is random, so compare the rows rather than the bytes.
        assertThat(uploaderRows).isEqualTo(serialRows).hasSize(rows.size());
        // Every chunk sent before the checkpoint was a whole one, sent by the uploader.
        assertThat(writesBeforeCheckpoint)
                .isNotEmpty()
                .allSatisfy(
                        write ->
                                assertThat(write).startsWith(UPLOADER).endsWith(" wrote " + CHUNK));
    }

    @Test
    void stagesParquetThroughTheUploader() throws Exception {
        RecordingStorage storage = new RecordingStorage();
        // A 1 MiB threshold sizes the row group to 1 MiB, so row groups flush, and files roll,
        // before
        // the checkpoint: their chunks and closes then go through the uploader.
        FileLoadsWriter<TestRow> writer =
                writer(
                        storage,
                        uploaderOptions()
                                .stagingFormat(StagingFormat.PARQUET)
                                .maxStagingFileBytes(1024L * 1024));
        // Parquet decides whether to flush a row group only from its hundredth record on, so many
        // modest rows rather than a few large ones.
        List<String> expected = names(600, 8 * 1024);
        for (int i = 0; i < expected.size(); i++) {
            writer.write(new TestRow("t1", expected.get(i), (long) i), CONTEXT);
        }
        List<String> writesBeforeCheckpoint = storage.writes();

        Collection<FileLoadsCommittable> committables = writer.prepareCommit();
        writer.close();

        assertThat(writesBeforeCheckpoint).anyMatch(write -> write.startsWith(UPLOADER));
        assertThat(committables).hasSizeGreaterThan(1);
        List<String> names = new ArrayList<>();
        for (FileLoadsCommittable committable : committables) {
            names.addAll(parquetRowNames(storage.object(committable.getUri())));
        }
        assertThat(names).containsExactlyElementsOf(expected);
    }

    private static List<String> parquetRowNames(byte[] staged) throws IOException {
        List<String> names = new ArrayList<>();
        try (ParquetReader<GenericRecord> reader =
                AvroParquetReader.<GenericRecord>builder(
                                new FileLoadsWriterTest.BytesInputFile(staged))
                        .build()) {
            for (GenericRecord record = reader.read(); record != null; record = reader.read()) {
                names.add(String.valueOf(record.get("name")));
            }
        }
        return names;
    }

    @Test
    void prepareCommitWaitsForAChunkStillOnTheUploader() throws Exception {
        RecordingStorage storage = new RecordingStorage();
        FileLoadsWriter<TestRow> writer = writer(storage, uploaderOptions());
        // Holds only the uploader's writes, so a checkpoint that skipped the drain could write and
        // finalize on its own thread and return under the held chunk.
        storage.holdWrites();
        // One row compresses to less than a chunk; two fill at least one.
        List<String> expected = largeNames(2);
        for (int i = 0; i < expected.size(); i++) {
            writer.write(new TestRow("t1", expected.get(i), (long) i), CONTEXT);
        }
        assertThat(storage.awaitHeld()).isTrue();

        CompletableFuture<Collection<FileLoadsCommittable>> commit =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return writer.prepareCommit();
                            } catch (Exception e) {
                                throw new IllegalStateException(e);
                            }
                        });

        // A checkpoint that did not drain would return, or finalize under the held chunk.
        assertThatThrownBy(() -> commit.get(200, TimeUnit.MILLISECONDS))
                .isInstanceOf(TimeoutException.class);
        storage.release();
        Collection<FileLoadsCommittable> committables = commit.get(5, TimeUnit.SECONDS);
        writer.close();

        // The held chunk was written before the file's last chunk and close, so the object reads
        // back whole and in order.
        assertThat(committables).hasSize(1);
        assertThat(rowNames(storage.object(committables.iterator().next().getUri())))
                .containsExactlyElementsOf(expected);
    }

    @Test
    void checkpointFinalizesTheOpenFilesOnTheCallingThreadAfterTheUploaderDrains()
            throws Exception {
        RecordingStorage storage = new RecordingStorage();
        FileLoadsWriter<TestRow> writer = writer(storage, uploaderOptions().maxOpenDestinations(2));
        writer.write(new TestRow("t1", "a", 1L), CONTEXT);
        writer.write(new TestRow("t2", "b", 2L), CONTEXT);
        String caller = Thread.currentThread().getName();

        Collection<FileLoadsCommittable> committables = writer.prepareCommit();
        writer.close();

        assertThat(committables).hasSize(2);
        assertThat(storage.closeThreads()).containsExactly(caller, caller);
        for (FileLoadsCommittable committable : committables) {
            assertThat(rowNames(storage.object(committable.getUri()))).hasSize(1);
        }
    }

    @Test
    void concurrentCheckpointFinalizationClosesDetachedFilesOffBothThreads() throws Exception {
        RecordingStorage storage = new RecordingStorage();
        FileLoadsWriter<TestRow> writer =
                writer(
                        storage,
                        uploaderOptions()
                                .maxOpenDestinations(3)
                                .maxConcurrentCheckpointFinalizations(3));
        writer.write(new TestRow("t1", "a", 1L), CONTEXT);
        writer.write(new TestRow("t2", "b", 2L), CONTEXT);
        writer.write(new TestRow("t3", "c", 3L), CONTEXT);
        String caller = Thread.currentThread().getName();

        Collection<FileLoadsCommittable> committables = writer.prepareCommit();
        writer.close();

        assertThat(committables).hasSize(3);
        // The checkpoint finalizer's workers close the files, neither the task thread nor the
        // uploader.
        assertThat(storage.closeThreads())
                .hasSize(3)
                .noneMatch(name -> name.equals(caller))
                .noneMatch(name -> name.startsWith(UPLOADER));
        List<String> names = new ArrayList<>();
        for (FileLoadsCommittable committable : committables) {
            names.addAll(rowNames(storage.object(committable.getUri())));
        }
        assertThat(names).containsExactlyInAnyOrder("a", "b", "c");
    }

    @Test
    void aFinalizationFailureOnTheUploaderFailsTheCheckpoint() throws Exception {
        RecordingStorage storage = new RecordingStorage();
        IOException cause = new IOException("finalize rejected");
        storage.failCloses(cause);
        // Held so the failure cannot surface before the write that rolls the file returns.
        storage.holdCloses();
        FileLoadsWriter<TestRow> writer = writer(storage, uploaderOptions().maxStagingFileBytes(1));
        writer.write(new TestRow("t1", "a", 1L), CONTEXT);
        storage.release();

        assertThatThrownBy(writer::prepareCommit).isInstanceOf(IOException.class).hasCause(cause);
        writer.close();
        assertThat(storage.closedUris()).isEmpty();
    }

    @Test
    void anErrorOnTheUploaderReachesTheCheckpointAsItself() throws Exception {
        RecordingStorage storage = new RecordingStorage();
        NoClassDefFoundError error = new NoClassDefFoundError("finalize blew up");
        storage.failCloses(error);
        storage.holdCloses();
        FileLoadsWriter<TestRow> writer = writer(storage, uploaderOptions().maxStagingFileBytes(1));
        writer.write(new TestRow("t1", "a", 1L), CONTEXT);
        storage.release();

        assertThatThrownBy(writer::prepareCommit).isSameAs(error);
        writer.close();
    }

    @Test
    void finalizationsStillOnTheUploaderCountTowardsMaxPendingFiles() throws Exception {
        RecordingStorage storage = new RecordingStorage();
        storage.holdCloses();
        FileLoadsWriter<TestRow> writer =
                writer(
                        storage,
                        uploaderOptions()
                                .maxStagingFileBytes(1)
                                .maxOpenDestinations(1)
                                .maxPendingFiles(2));
        writer.write(new TestRow("t1", "a", 1L), CONTEXT);
        writer.write(new TestRow("t1", "b", 2L), CONTEXT);

        assertThatThrownBy(() -> writer.write(new TestRow("t1", "c", 3L), CONTEXT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("maxPendingFiles=2");

        writer.close();
        storage.release();
    }

    @Test
    void closeDropsQueuedFinalizationsWithoutWaitingForThem() throws Exception {
        RecordingStorage storage = new RecordingStorage();
        // Ignores the interrupt close() sends, so only a close that waited could see it end.
        storage.holdClosesUninterruptibly();
        FileLoadsWriter<TestRow> writer = writer(storage, uploaderOptions().maxStagingFileBytes(1));
        writer.write(new TestRow("t1", "a", 1L), CONTEXT);
        writer.write(new TestRow("t2", "b", 2L), CONTEXT);
        assertThat(storage.awaitHeld()).isTrue();

        writer.close();

        assertThat(storage.hasExited()).isFalse();
        storage.release();
        assertThat(storage.awaitExited()).isTrue();
        // Once the thread has ended, a queued close that had survived the close would have run.
        awaitUploaderThreadsEnded(UPLOADER);
        // Only the close that was already running finalized; the queued one never ran.
        assertThat(storage.closedUris())
                .singleElement()
                .satisfies(uri -> assertThat(uri).contains("/p.d.t1/"));
    }

    @Test
    void withoutAnUploaderTheDefaultChunkReachesTheStorage() throws Exception {
        RecordingStorage storage = new RecordingStorage();
        FileLoadsWriter<TestRow> writer =
                writer(storage, FileLoadsOptions.builder().stagingPath("gs://bucket/prefix"));
        writer.write(new TestRow("t1", "a", 1L), CONTEXT);
        String caller = Thread.currentThread().getName();

        assertThat(writer.prepareCommit()).hasSize(1);
        writer.close();

        assertThat(storage.chunkSizes()).containsExactly(4 * 1024 * 1024);
        assertThat(storage.closeThreads()).containsExactly(caller);
    }

    @Test
    void plannedUploadBuffersCountEveryOpenFileAndUploaderOperationWithoutOverflowing() {
        // The defaults: 16 open files of 4 MiB and no uploader.
        assertThat(FileLoadsWriter.plannedUploadBufferBytes(16, 0, 4L * 1024 * 1024))
                .isEqualTo(64L * 1024 * 1024);
        assertThat(FileLoadsWriter.plannedUploadBufferBytes(16, 2, 4L * 1024 * 1024))
                .isEqualTo(72L * 1024 * 1024);
        // Every accepted option fits a long; a product that would not saturates instead of
        // wrapping to a small or negative size that would hide the warning.
        assertThat(
                        FileLoadsWriter.plannedUploadBufferBytes(
                                Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE))
                .isEqualTo(2L * Integer.MAX_VALUE * Integer.MAX_VALUE);
        assertThat(FileLoadsWriter.plannedUploadBufferBytes(4, 0, Long.MAX_VALUE / 2))
                .isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void warnsOnlyAboveAQuarterOfABoundedHeap() {
        long heap = 1024L * 1024 * 1024;
        assertThat(FileLoadsWriter.uploadBuffersExceedHeapShare(heap / 4, heap)).isFalse();
        assertThat(FileLoadsWriter.uploadBuffersExceedHeapShare(heap / 4 + 1, heap)).isTrue();
        // A JVM without a heap limit reports Long.MAX_VALUE, against which nothing is too much.
        assertThat(FileLoadsWriter.uploadBuffersExceedHeapShare(Long.MAX_VALUE, Long.MAX_VALUE))
                .isFalse();
    }

    @Test
    void warnsAtStartOnlyWhenTheUploaderTipsTheWriterOverAQuarterOfTheHeap() throws Exception {
        long chunk = 4L * 1024 * 1024;
        long maxHeap = Runtime.getRuntime().maxMemory();
        long quarter = maxHeap / 4;
        // Open files alone stay within the quarter; two uploader operations more cross it. A JVM
        // with
        // no heap limit never warns, and one under 32 MiB or beyond an int's worth of chunks leaves
        // no valid destination count to bracket the threshold with, so those runs are skipped.
        long bracket = quarter / chunk - 1;
        assumeTrue(maxHeap != Long.MAX_VALUE && bracket >= 1 && bracket <= Integer.MAX_VALUE);
        int openFiles = (int) bracket;
        FileLoadsOptions.Builder options =
                FileLoadsOptions.builder()
                        .stagingPath("gs://bucket/prefix")
                        .stagingUploadChunkBytes(chunk)
                        .maxOpenDestinations(openFiles)
                        .maxPendingFiles(
                                Math.max(openFiles, FileLoadsOptions.DEFAULT_MAX_PENDING_FILES));

        try (LogCapture capture = LogCapture.of(FileLoadsWriter.class)) {
            writer(new RecordingStorage(), options).close();
            assertThat(capture.getMessages()).noneMatch(m -> m.contains("staging upload buffers"));

            writer(
                            new RecordingStorage(),
                            options.backgroundStagingUploadEnabled(true)
                                    .maxInFlightUploadOperations(2))
                    .close();
            assertThat(capture.getMessages())
                    .filteredOn(m -> m.contains("staging upload buffers"))
                    .singleElement()
                    .asString()
                    .contains(openFiles + " maxOpenDestinations")
                    .contains("2 maxInFlightUploadOperations")
                    .contains(chunk + "-byte stagingUploadChunkBytes");
        }
    }

    /**
     * Waits for every uploader thread with this name prefix to end, so that anything a stopped
     * uploader still ran has happened before the assertions that follow.
     */
    private static void awaitUploaderThreadsEnded(String prefix) throws InterruptedException {
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.getName().startsWith(prefix)) {
                thread.join(TimeUnit.SECONDS.toMillis(5));
                assertThat(thread.isAlive()).as(thread.getName()).isFalse();
            }
        }
    }

    private static FileLoadsOptions.Builder uploaderOptions() {
        return FileLoadsOptions.builder()
                .stagingPath("gs://bucket/prefix")
                .stagingUploadChunkBytes(CHUNK)
                .backgroundStagingUploadEnabled(true)
                .maxInFlightUploadOperations(2);
    }

    private static FileLoadsWriter<TestRow> writer(
            StagingStorage storage, FileLoadsOptions.Builder options) {
        return new FileLoadsWriter<>(
                FileLoadsWriterTest.config(FailureHandler.failJob()),
                options.build(),
                storage,
                TestSinkWriterMetricGroup.create(),
                "0123456789abcdef0123456789abcdef",
                3,
                1,
                new ManualProcessingTimeService());
    }

    /** Rows of random printable text, about 300 KiB each, which zstandard shrinks only a little. */
    private static List<TestRow> largeRows(int count) {
        List<String> names = largeNames(count);
        List<TestRow> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            rows.add(new TestRow("t1", names.get(i), (long) i));
        }
        return rows;
    }

    private static List<String> largeNames(int count) {
        return names(count, 300 * 1024);
    }

    private static List<String> names(int count, int chars) {
        Random random = new Random(2021);
        List<String> names = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            StringBuilder name = new StringBuilder();
            for (int c = 0; c < chars; c++) {
                name.append((char) ('!' + random.nextInt(90)));
            }
            names.add(name.toString());
        }
        return names;
    }

    private static List<String> commitAndRead(
            FileLoadsWriter<TestRow> writer, RecordingStorage storage) throws Exception {
        List<String> names = new ArrayList<>();
        for (FileLoadsCommittable committable : writer.prepareCommit()) {
            names.addAll(rowNames(storage.object(committable.getUri())));
        }
        writer.close();
        return names;
    }

    private static List<String> rowNames(byte[] staged) throws IOException {
        List<String> names = new ArrayList<>();
        try (DataFileReader<GenericRecord> reader =
                new DataFileReader<>(
                        new SeekableByteArrayInput(staged), new GenericDatumReader<>())) {
            for (GenericRecord record : reader) {
                names.add(String.valueOf(record.get("name")));
            }
        }
        return names;
    }

    /**
     * Staging storage over plain channels that record which thread wrote and finalized what. Unlike
     * {@code Channels.newChannel}, an interrupt never closes, and so never publishes, an object;
     * the real client behaves the same. Its writes or its closes can be held, and its closes
     * failed.
     */
    private static final class RecordingStorage implements StagingStorage {
        private static final long serialVersionUID = 1L;

        private final Map<String, byte[]> objects =
                Collections.synchronizedMap(new LinkedHashMap<>());
        private final List<String> closeThreads = Collections.synchronizedList(new ArrayList<>());
        private final List<String> writes = Collections.synchronizedList(new ArrayList<>());
        private final List<Integer> chunkSizes = Collections.synchronizedList(new ArrayList<>());
        private final CountDownLatch held = new CountDownLatch(1);
        private final CountDownLatch exited = new CountDownLatch(1);
        private volatile CountDownLatch gate;
        private volatile boolean holdWrites;
        private volatile boolean uninterruptible;
        private volatile Throwable closeFailure;

        void holdWrites() {
            holdWrites = true;
            gate = new CountDownLatch(1);
        }

        void holdCloses() {
            gate = new CountDownLatch(1);
        }

        void holdClosesUninterruptibly() {
            uninterruptible = true;
            holdCloses();
        }

        void release() {
            gate.countDown();
        }

        void failCloses(Throwable failure) {
            closeFailure = failure;
        }

        boolean awaitHeld() throws InterruptedException {
            return held.await(5, TimeUnit.SECONDS);
        }

        boolean awaitExited() throws InterruptedException {
            return exited.await(5, TimeUnit.SECONDS);
        }

        boolean hasExited() {
            return exited.getCount() == 0;
        }

        List<String> closedUris() {
            synchronized (objects) {
                return new ArrayList<>(objects.keySet());
            }
        }

        List<String> closeThreads() {
            synchronized (closeThreads) {
                return new ArrayList<>(closeThreads);
            }
        }

        /** Each write as its thread's name, {@code wrote} and its length. */
        List<String> writes() {
            synchronized (writes) {
                return new ArrayList<>(writes);
            }
        }

        List<Integer> chunkSizes() {
            synchronized (chunkSizes) {
                return new ArrayList<>(chunkSizes);
            }
        }

        byte[] object(String uri) {
            return objects.get(uri);
        }

        @Override
        public WritableByteChannel createObject(String gcsUri, int chunkBytes) {
            chunkSizes.add(chunkBytes);
            return new WritableByteChannel() {
                private final ByteArrayOutputStream content = new ByteArrayOutputStream();
                private boolean open = true;

                @Override
                public int write(ByteBuffer source) throws IOException {
                    if (holdWrites && Thread.currentThread().getName().startsWith(UPLOADER)) {
                        awaitGate();
                    }
                    int length = source.remaining();
                    byte[] copy = new byte[length];
                    source.get(copy);
                    content.write(copy, 0, length);
                    writes.add(Thread.currentThread().getName() + " wrote " + length);
                    return length;
                }

                @Override
                public boolean isOpen() {
                    return open;
                }

                @Override
                public void close() throws IOException {
                    try {
                        if (!holdWrites) {
                            awaitGate();
                        }
                        Throwable failure = closeFailure;
                        if (failure instanceof IOException) {
                            throw (IOException) failure;
                        }
                        if (failure instanceof Error) {
                            throw (Error) failure;
                        }
                        open = false;
                        closeThreads.add(Thread.currentThread().getName());
                        objects.put(gcsUri, content.toByteArray());
                    } finally {
                        exited.countDown();
                    }
                }
            };
        }

        private void awaitGate() throws IOException {
            CountDownLatch current = gate;
            if (current == null) {
                return;
            }
            held.countDown();
            boolean interrupted = false;
            try {
                while (true) {
                    try {
                        if (!current.await(5, TimeUnit.SECONDS)) {
                            throw new IOException("Timed out holding the storage");
                        }
                        return;
                    } catch (InterruptedException e) {
                        if (!uninterruptible) {
                            Thread.currentThread().interrupt();
                            throw new IOException("Interrupted holding the storage", e);
                        }
                        interrupted = true;
                    }
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public void deleteObjects(List<String> gcsUris) {}

        @Override
        public void close() {}
    }
}
