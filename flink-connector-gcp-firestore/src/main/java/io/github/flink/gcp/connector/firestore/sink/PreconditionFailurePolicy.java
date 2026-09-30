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

package io.github.flink.gcp.connector.firestore.sink;

import org.apache.flink.annotation.PublicEvolving;

/**
 * What the sink does with a write Firestore refuses because its {@code lastUpdateTime} precondition
 * no longer holds.
 *
 * <p>The policy covers exactly that refusal: {@code FAILED_PRECONDITION} answering a write that
 * carries a precondition. The same status answering a write <em>without</em> one always fails the
 * job, because it then says something about the database rather than about the record — a database
 * in Datastore mode is reported to refuse every Firestore API write with it.
 *
 * <p>This is a policy rather than a fixed rule because both readings of the refusal are defensible,
 * and only the pipeline's owner knows which applies. A pipeline that writes conditionally usually
 * expects some writes to lose the race and wants them captured while the job keeps running, which
 * is what a failure handler is for. But a replayed conditional write is refused the same way — its
 * first application changed the update time — so under a dropping handler a restart would drop what
 * the replay re-sent, and a stream in which every precondition fails says the pipeline reads stale
 * update times, which shedding records one at a time would hide behind a green job.
 *
 * <p>The default is {@link #FAIL_JOB} because it is the conservative one: it cannot lose a record,
 * and it makes the problem visible immediately.
 *
 * @see FirestoreSinkBuilder#preconditionFailurePolicy(PreconditionFailurePolicy)
 */
@PublicEvolving
public enum PreconditionFailurePolicy {

    /**
     * Fail the job. The record is not lost — it is replayed from the source on restart — but a
     * stream whose preconditions keep failing will not make progress until they are fixed.
     */
    FAIL_JOB,

    /**
     * Hand the write to the configured {@code failedWriteHandler}, like any other per-write
     * refusal. What happens then is that handler's decision: {@code FailureHandler.failJob()} still
     * fails the job, {@code logAndDrop()} drops the record, and {@code sendToDeadLetterQueue(...)}
     * captures it.
     */
    ROUTE_TO_FAILURE_HANDLER
}
