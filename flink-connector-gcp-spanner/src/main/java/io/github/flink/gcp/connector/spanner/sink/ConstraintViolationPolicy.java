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

package io.github.flink.gcp.connector.spanner.sink;

import org.apache.flink.annotation.PublicEvolving;

/**
 * What the sink does with a mutation Spanner refuses for violating a constraint.
 *
 * <p>Which refusals those are, measured rather than assumed (2026-08-09, one run, emulator
 * v1.5.56): a {@code NULL} in a {@code NOT NULL} column, a value past its column's length and a
 * foreign-key violation come back as {@code FAILED_PRECONDITION}; a {@code CHECK} constraint comes
 * back as {@code OUT_OF_RANGE}. This policy covers both statuses. A duplicate key or a unique-index
 * collision is <em>not</em> among them — that is {@code ALREADY_EXISTS}, which is always routed.
 *
 * <p>This is a policy rather than a fixed rule because the two readings of such a refusal are both
 * defensible and only the pipeline's owner knows which applies. A constraint violation usually says
 * the mapping from records to columns is wrong — every record of that shape will be refused, so
 * shedding them one at a time hides a systematic problem behind a green job. But a stream that
 * genuinely carries occasional records the schema will not accept wants them captured and the job
 * kept running, which is what a failure handler is for.
 *
 * <p>The default is {@link #FAIL_JOB} because it is the conservative one: it cannot lose a record,
 * and it makes the problem visible immediately. There is a second reason to keep it the default.
 * Spanner answers <em>every</em> write with {@code FAILED_PRECONDITION} while its CMEK key is
 * disabled, destroyed or unreachable, and restores service by itself when the key comes back — so a
 * job that routed this status into a dropping handler would shed its whole stream during a key
 * incident rather than waiting it out.
 *
 * @see SpannerSinkBuilder#constraintViolationPolicy(ConstraintViolationPolicy)
 */
@PublicEvolving
public enum ConstraintViolationPolicy {

    /**
     * Fail the job. The record is not lost — it is replayed from the source on restart — but a
     * stream that keeps producing such records will not make progress until the constraint or the
     * mapping is fixed.
     */
    FAIL_JOB,

    /**
     * Hand the mutation to the configured {@code failedMutationHandler}, like any other per-record
     * refusal. What happens then is that handler's decision: {@code FailureHandler.failJob()} still
     * fails the job, {@code logAndDrop()} drops the record, and {@code sendToDeadLetterQueue(...)}
     * captures it. Choosing this and a dropping handler is a decision to lose records the schema
     * refuses, which is exactly what makes it worth stating in the job rather than assuming.
     */
    ROUTE_TO_FAILURE_HANDLER
}
