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

package io.github.flink.gcp.connector.bigtable.sink;

import java.util.IdentityHashMap;
import java.util.Map;

/** Complete synchronous operator notifications, including gaps between commit invocations. */
final class Stage2NotificationProgress {
    private final Map<Thread, Notification> active = new IdentityHashMap<>();
    private long started;
    private long finished;
    private long failed;
    private long maxFinishedNanos;
    private long maxInvocations;
    private long maxEntries;
    private long outsideNotificationInvocations;

    synchronized void started(long checkpoint, long now) {
        if (active.containsKey(Thread.currentThread())) {
            throw new IllegalStateException("Nested checkpoint completion notification");
        }
        active.put(Thread.currentThread(), new Notification(checkpoint, now));
        started++;
    }

    synchronized void invocation(int entries) {
        Notification current = active.get(Thread.currentThread());
        if (current == null) {
            outsideNotificationInvocations++;
        } else {
            current.invocations++;
            current.entries += entries;
        }
    }

    synchronized void finished(boolean successful, long now) {
        Notification current = active.remove(Thread.currentThread());
        if (current == null) {
            throw new IllegalStateException("No checkpoint completion notification active");
        }
        finished++;
        if (!successful) {
            failed++;
        }
        maxFinishedNanos = Math.max(maxFinishedNanos, now - current.started);
        maxInvocations = Math.max(maxInvocations, current.invocations);
        maxEntries = Math.max(maxEntries, current.entries);
    }

    synchronized String sample(long now) {
        StringBuilder records = new StringBuilder("[");
        for (Notification current : active.values()) {
            if (records.length() > 1) {
                records.append(',');
            }
            records.append("{\"checkpointId\":")
                    .append(current.checkpoint)
                    .append(",\"ageNanos\":")
                    .append(Math.max(0, now - current.started))
                    .append(",\"invocations\":")
                    .append(current.invocations)
                    .append(",\"entries\":")
                    .append(current.entries)
                    .append('}');
        }
        return "{\"activeNotifications\":"
                + records.append(']')
                + ",\"startedNotifications\":"
                + started
                + ",\"finishedNotifications\":"
                + finished
                + ",\"failedNotifications\":"
                + failed
                + ",\"maxFinishedNotificationNanos\":"
                + maxFinishedNanos
                + ",\"maxNotificationInvocations\":"
                + maxInvocations
                + ",\"maxNotificationEntries\":"
                + maxEntries
                + ",\"outsideNotificationInvocations\":"
                + outsideNotificationInvocations
                + "}";
    }

    private static final class Notification {
        final long checkpoint;
        final long started;
        long invocations;
        long entries;

        Notification(long checkpoint, long started) {
            this.checkpoint = checkpoint;
            this.started = started;
        }
    }
}
