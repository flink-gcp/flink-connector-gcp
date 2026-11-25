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
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.UserCodeClassLoader;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link UserCodeClassLoader} over a given class loader that keeps one release hook per name and
 * runs them on {@link #release()}, as Flink does before it closes a job's loader. Unlike Flink,
 * which logs a hook's failure and runs the rest, a hook that throws here propagates, so a test sees
 * it.
 *
 * <p>Flink hands each call to {@code getUserCodeClassLoader()} a new wrapper around the same
 * loader; a test that models a second call wraps the same loader in a second instance.
 *
 * <p>Implemented here rather than through {@code SimpleUserCodeClassLoader}: that class is
 * unannotated and ignores the hooks, and the interface is {@code @PublicEvolving} with two methods.
 */
@Internal
public final class FakeUserCodeClassLoader implements UserCodeClassLoader {

    private final ClassLoader classLoader;
    private final Map<String, Runnable> releaseHooks = new LinkedHashMap<>();

    /**
     * Creates the fake.
     *
     * @param classLoader the loader {@link #asClassLoader()} returns
     */
    public FakeUserCodeClassLoader(ClassLoader classLoader) {
        this.classLoader = Preconditions.checkNotNull(classLoader, "classLoader must not be null");
    }

    @Override
    public ClassLoader asClassLoader() {
        return classLoader;
    }

    @Override
    public synchronized void registerReleaseHookIfAbsent(
            String releaseHookName, Runnable releaseHook) {
        releaseHooks.putIfAbsent(releaseHookName, releaseHook);
    }

    /**
     * Returns the names of the hooks registered and not yet run, in registration order.
     *
     * @return the names
     */
    public synchronized List<String> releaseHookNames() {
        return new ArrayList<>(releaseHooks.keySet());
    }

    /**
     * Runs the registered hooks and forgets them, as Flink does when it releases the loader. Unlike
     * Flink, which releases a loader once, this can be called again, and a hook registered after a
     * call runs on the next one. The hooks run in registration order here, where Flink promises no
     * order, so a test must not depend on it.
     */
    public void release() {
        List<Runnable> hooks;
        synchronized (this) {
            hooks = new ArrayList<>(releaseHooks.values());
            releaseHooks.clear();
        }
        hooks.forEach(Runnable::run);
    }
}
