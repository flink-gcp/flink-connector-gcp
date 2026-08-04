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

package io.github.flink.gcp.connector.base.lifecycle;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Releasing several resources at once, so that one refusing to close never strands the rest. */
@Internal
public final class Closers {

    private Closers() {}

    /**
     * Closes every given resource, then reports the first failure with any later one suppressed
     * onto it.
     *
     * <p>{@code null} entries are skipped, so a caller can pass a local it had not reached yet. A
     * null <em>collection</em> is not, unlike {@code IOUtils.closeAll} — nothing here can produce
     * one, and a caller that does has a bug worth surfacing where it happens rather than silently
     * closing nothing. A resource is never left open because an earlier one refused to close — the
     * loop finishes before anything is thrown, which is what lets every {@code close()} in this
     * repository put its failure handler last and still have it closed.
     *
     * <p><b>The reported failure keeps its own type</b>, and that is the whole reason this loop is
     * written out rather than delegated to Flink's {@code IOUtils.closeAll(Iterable, Class)}. That
     * method rethrows from inside its loop anything the given class does not cover, so {@code
     * Exception.class} — what its one-argument form passes — abandons every later resource on an
     * {@code Error}, which is the bug #276 fixed at nine call sites. Its {@code Throwable.class}
     * form closes everything, but collects a non-{@code Exception} as {@code new Exception(e)}:
     * Flink's {@code Task.preProcessException} tests the throwable itself and halts the JVM on
     * {@code isJvmFatalError(t) || t instanceof OutOfMemoryError}, so a wrapped {@code
     * OutOfMemoryError} from a teardown would fail the task into a restart loop instead of taking
     * the TaskManager down. {@link ExceptionUtils#rethrowException(Throwable)} throws an {@code
     * Error} as an {@code Error}.
     *
     * @param closeables the resources to release, in order; entries may be {@code null}
     * @throws Exception the first close failure, or an {@code Error} thrown as itself
     */
    public static void closeAll(Iterable<? extends AutoCloseable> closeables) throws Exception {
        Throwable collected = null;
        for (AutoCloseable closeable : closeables) {
            try {
                if (closeable != null) {
                    closeable.close();
                }
            } catch (Throwable e) {
                collected = ExceptionUtils.firstOrSuppressed(e, collected);
            }
        }
        if (collected != null) {
            ExceptionUtils.rethrowException(collected);
        }
    }

    /**
     * Closes every given resource, then reports the first failure with any later one suppressed
     * onto it.
     *
     * @param closeables the resources to release, in order; entries may be {@code null}
     * @throws Exception the first close failure, or an {@code Error} thrown as itself
     * @see #closeAll(Iterable)
     */
    public static void closeAll(AutoCloseable... closeables) throws Exception {
        closeAll(Arrays.asList(closeables));
    }

    /**
     * Closes every given resource, reporting any close failure as a suppressed exception on the
     * failure already in flight rather than in place of it.
     *
     * <p>The caller keeps its own exception and rethrows it: what went wrong is why the resources
     * are being released, so it must not be replaced by whatever a close then did. {@code null}
     * entries are skipped, so a caller can pass a local it had not reached yet.
     *
     * <p>This is the one thing {@link #closeAll(Iterable)} does not do: it throws its collected
     * failure, which is right when closing <em>is</em> the operation and wrong when something else
     * already failed. <b>Catching {@code Throwable} is what makes this complete</b> — {@code
     * closeAll} rethrows an {@code Error} as an {@code Error}, and one escaping here would skip the
     * caller's rethrow and replace its failure, the leak this class exists to prevent reached
     * through it.
     *
     * <p><b>A JVM-fatal close failure is the one exception</b>, and it takes {@code failure}'s
     * place with {@code failure} suppressed onto it. Flink's {@code Task.preProcessException}
     * inspects only the throwable it is handed, so one arriving as a suppressed entry is one
     * nothing halts on — and for an {@code OutOfMemoryError} that silently overrides the operator's
     * {@code taskmanager.jvm-exit-on-oom}, which is the same shape of defect as #276 itself. The
     * set is Flink's own {@link ExceptionUtils#isJvmFatalOrOutOfMemoryError(Throwable)}, so it is
     * narrow: {@code NoClassDefFoundError} — the realistic first-classload failure — is not in it
     * and is suppressed like anything else. Escalation happens after the loop has finished, so
     * every resource is still closed first.
     *
     * <p>The bound, stated because it is known rather than overlooked: what is inspected is the
     * throwable {@link #closeAll(Iterable)} reports, which is the <em>first</em> close failure. Two
     * closes failing where only the second is fatal leaves that one suppressed and unescalated.
     * Reordering which failure is reported to chase it would cost {@code closeAll} its "the first
     * failure wins" contract for a doubly-rare case, and Flink inspects only top-level throwables
     * everywhere anyway.
     *
     * <p>Two or more close failures arrive nested rather than as siblings — the second is
     * suppressed onto the first, which is suppressed onto {@code failure}. Nothing is lost;
     * printing a stack trace walks suppressed exceptions recursively.
     *
     * @param failure the exception the caller is about to rethrow
     * @param first the first resource to release; may be {@code null}
     * @param rest any further resources, released in order; entries may be {@code null}
     */
    public static void closeAllSuppressing(
            Throwable failure, @Nullable AutoCloseable first, AutoCloseable... rest) {
        List<AutoCloseable> closeables = new ArrayList<>(rest.length + 1);
        closeables.add(first);
        closeables.addAll(Arrays.asList(rest));
        try {
            closeAll(closeables);
        } catch (Throwable e) {
            if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(e)) {
                e.addSuppressed(failure);
                // rethrow rather than a cast to Error: every member of that set is one today, and
                // this does not become a ClassCastException during teardown if Flink widens it.
                ExceptionUtils.rethrow(e);
            }
            failure.addSuppressed(e);
        }
    }
}
