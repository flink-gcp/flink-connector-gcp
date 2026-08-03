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
import org.apache.flink.util.IOUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Releasing resources on a path that is already failing. */
@Internal
public final class Closers {

    private Closers() {}

    /**
     * Closes every given resource, reporting any close failure as a suppressed exception on the
     * failure already in flight rather than in place of it.
     *
     * <p>The caller keeps its own exception and rethrows it: what went wrong is why the resources
     * are being released, so it must not be replaced by whatever a close then did. {@code null}
     * entries are skipped, so a caller can pass a local it had not reached yet.
     *
     * <p>This is the one thing {@link IOUtils#closeAll(Iterable, Class)} — which every writer's
     * {@code close()} here uses — does not do: it throws its collected failure, which is right when
     * closing <em>is</em> the operation and wrong when something else already failed.
     *
     * <p><b>{@code Throwable.class} is the load-bearing argument.</b> The one-argument {@code
     * closeAll(AutoCloseable...)} passes {@code Exception.class}, and anything the class does not
     * cover is rethrown from inside the loop — so an {@code Error} from one {@code close()} would
     * leave every later resource open <em>and</em> escape this method, skipping the caller's
     * rethrow. That is the leak this class exists to prevent, reached through it. Passing {@code
     * Throwable.class} makes the test always pass, so every resource is closed and everything is
     * collected; a non-{@code Exception} arrives wrapped, which is why catching {@code Exception}
     * below is complete.
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
            IOUtils.closeAll(closeables, Throwable.class);
        } catch (Exception e) {
            failure.addSuppressed(e);
        }
    }
}
