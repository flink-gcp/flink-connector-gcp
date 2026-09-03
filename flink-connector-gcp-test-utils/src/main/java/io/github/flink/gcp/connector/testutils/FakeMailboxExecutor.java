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
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingRunnable;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;

/**
 * A queue-backed {@link MailboxExecutor} for tests: {@link #execute} enqueues mails, {@link
 * #yield()} runs the next mail (blocking until one arrives, like the real mailbox), {@link
 * #drain()} runs every mail already enqueued, and {@link #quiesce()} makes every later {@link
 * #execute} throw the {@link RejectedExecutionException} the real mailbox throws once the task has
 * quiesced it, which happens before the operators close.
 */
@Internal
public final class FakeMailboxExecutor implements MailboxExecutor {

    private final LinkedBlockingDeque<ThrowingRunnable<? extends Exception>> mails =
            new LinkedBlockingDeque<>();

    private volatile boolean quiesced;

    @Override
    public void execute(
            MailOptions options,
            ThrowingRunnable<? extends Exception> command,
            String descriptionFormat,
            Object... descriptionArgs) {
        if (quiesced) {
            throw new RejectedExecutionException(
                    "The mailbox is quiesced: "
                            + String.format(descriptionFormat, descriptionArgs));
        }
        mails.add(command);
    }

    /**
     * Refuses every mail from now on, as the real mailbox does after the task's {@code
     * prepareClose()}. Mails already enqueued still run through {@link #yield()} and {@link
     * #drain()}.
     */
    public void quiesce() {
        quiesced = true;
    }

    @Override
    public void yield() throws InterruptedException, FlinkRuntimeException {
        run(mails.take());
    }

    @Override
    public boolean tryYield() throws FlinkRuntimeException {
        ThrowingRunnable<? extends Exception> mail = mails.poll();
        if (mail == null) {
            return false;
        }
        run(mail);
        return true;
    }

    @Override
    public boolean shouldInterrupt() {
        return false;
    }

    /** Runs every mail already enqueued, mimicking the idle mailbox loop between records. */
    public void drain() {
        while (tryYield()) {
            // Mails run for their side effects.
        }
    }

    private static void run(ThrowingRunnable<? extends Exception> mail) {
        try {
            mail.run();
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
    }
}
