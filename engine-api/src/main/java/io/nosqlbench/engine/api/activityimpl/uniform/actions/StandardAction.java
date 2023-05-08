/*
 * Copyright (c) 2022-2023 nosqlbench
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

package io.nosqlbench.engine.api.activityimpl.uniform.actions;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import io.nosqlbench.engine.api.activityapi.core.ActivityDefObserver;
import io.nosqlbench.engine.api.activityapi.core.SyncAction;
import io.nosqlbench.engine.api.activityapi.errorhandling.modular.ErrorDetail;
import io.nosqlbench.engine.api.activityapi.errorhandling.modular.NBErrorHandler;
import io.nosqlbench.engine.api.activityapi.planning.OpSequence;
import io.nosqlbench.api.engine.activityimpl.ActivityDef;
import io.nosqlbench.engine.api.activityimpl.OpDispenser;
import io.nosqlbench.engine.api.activityimpl.uniform.StandardActivity;
import io.nosqlbench.engine.api.activityimpl.uniform.flowtypes.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * This is the generified version of an Action. All driver adapters us this, as opposed
 * to previous NB versions where it was implemented for each driver.
 *
 * This allows the API to be consolidated so that the internal machinery of NB
 * works in a very consistent and uniform way for all users and drivers.
 *
 * @param <A> The type of activity
 * @param <R> The type of operation
 */
public class StandardAction<A extends StandardActivity<R, ?>, R extends Op> implements SyncAction, ActivityDefObserver {
    private final static Logger logger = LogManager.getLogger("ACTION");

    private final A activity;
    private final int slot;
    private final Timer executeTimer;
    private final Histogram triesHistogram;
    private final Timer resultSuccessTimer;
    private final Timer resultTimer;
    private final Timer bindTimer;
    private final NBErrorHandler errorHandler;
    private final OpSequence<OpDispenser<? extends Op>> opsequence;
    private final int maxTries;

    public StandardAction(A activity, int slot) {
        this.activity = activity;
        this.opsequence = activity.getOpSequence();
        this.slot = slot;
        this.maxTries = activity.getMaxTries();
        bindTimer = activity.getInstrumentation().getOrCreateBindTimer();
        executeTimer = activity.getInstrumentation().getOrCreateExecuteTimer();
        triesHistogram = activity.getInstrumentation().getOrCreateTriesHistogram();
        resultTimer = activity.getInstrumentation().getOrCreateResultTimer();
        resultSuccessTimer = activity.getInstrumentation().getOrCreateResultSuccessTimer();
        errorHandler = activity.getErrorHandler();
    }

    @Override
    public int runCycle(long cycle) {

        OpDispenser<? extends Op> dispenser;
        Op op = null;

        try (Timer.Context ct = bindTimer.time()) {
            dispenser = opsequence.apply(cycle);
            op = dispenser.apply(cycle);
        } catch (Exception e) {
            throw new RuntimeException("while binding request in cycle " + cycle + ": " + e.getMessage(), e);
        }

        int code = 0;
        Object result = null;
        while (op != null) {

            int tries = 0;
            while (tries++ < maxTries) {
                Throwable error = null;
                long startedAt = System.nanoTime();

                dispenser.onStart(cycle);

                try (Timer.Context ct = executeTimer.time()) {
                    if (op instanceof RunnableOp) {
                        ((RunnableOp) op).run();
                    } else if (op instanceof CycleOp<?>) {
                        result = ((CycleOp) op).apply(cycle);
                    } else if (op instanceof ChainingOp) {
                        result = ((ChainingOp) op).apply(result);
                    } else {
                        throw new RuntimeException("The op implementation did not implement any active logic. Implement " +
                            "one of [RunnableOp, CycleOp, or ChainingOp]");
                    }
                } catch (Exception e) {
                    error = e;
                } finally {
                    long nanos = System.nanoTime() - startedAt;
                    resultTimer.update(nanos, TimeUnit.NANOSECONDS);
                    if (error == null) {
                        resultSuccessTimer.update(nanos, TimeUnit.NANOSECONDS);
                        dispenser.onSuccess(cycle, nanos, op.getResultSize());

                        if (dispenser.getExpectedResultExpression() != null) { // TODO JK refactor the whole if/else break/continue tree
                            if (op.verified()) { // TODO JK Could this be moved to BaseOpDispenser?
                                logger.info(() -> "Verification of result passed"); // TODO/MVEL: this is too verbose per cycle
                                break;
                            } else {
                                // retry
                                var triesLeft = maxTries - tries;
                                logger.info("Verification of result did not pass - {} retries left", triesLeft);
                                // TODO/MVEL: I think we should designate a separate logging channel for verification logic
                                if (triesLeft == 0) {
                                    var retriesExhausted = new RuntimeException("Max retries for verification step exhausted."); // TODO JK do we need a dedicated exception here? VerificationRetriesExhaustedException?
                                    var errorDetail = errorHandler.handleError(retriesExhausted, cycle, nanos);
                                    dispenser.onError(cycle, nanos, retriesExhausted);
                                    code = ErrorDetail.ERROR_RETRYABLE.resultCode; // TODO JK use code from errorDetail.resultCode?
                                    break;
                                }
                                continue;
                                // TODO/MVEL: I think we should collapse all this if possible to throwing a UnverifiedError and let error handlers do their thing.
                                // TODO/MVEL: This would work nicely with existing mechanisms and allow users to route errors and status codes as they like.
                                // TODO/MVEL: A future refinement would be to allow customized error handlers (+-) per dispenser
                            }
                        } else {
                            break;
                        }
                    } else {
                        ErrorDetail detail = errorHandler.handleError(error, cycle, nanos);
                        dispenser.onError(cycle, nanos, error);
                        code = detail.resultCode;
                        if (!detail.isRetryable()) {
                            break;
                        }
                    }
                }
            }
            triesHistogram.update(tries);

            if (op instanceof OpGenerator) {
                logger.trace(() -> "GEN OP for cycle(" + cycle + ")");
                op = ((OpGenerator) op).getNextOp();
            } else {
                op = null;
            }
        }

        return code;
    }

    @Override
    public void onActivityDefUpdate(ActivityDef activityDef) {
    }
}
