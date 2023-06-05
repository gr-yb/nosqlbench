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

package io.nosqlbench.adapter.s4r.util;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import io.nosqlbench.adapter.s4r.dispensers.AmqpBaseOpDispenser;
import io.nosqlbench.api.config.NBLabeledElement;
import io.nosqlbench.api.config.NBLabels;
import io.nosqlbench.api.engine.metrics.ActivityMetrics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class S4RAdapterMetrics {

    private static final Logger logger = LogManager.getLogger("S4JAdapterMetrics");
    private final NBLabels labels;

    private Histogram messageSizeHistogram;
    private Timer bindTimer;
    private Timer executeTimer;
    // - message out of sequence error counter
    private Counter msgErrOutOfSeqCounter;
    // - message loss counter
    private Counter msgErrLossCounter;
    // - message duplicate error counter
    private Counter msgErrDuplicateCounter;

    public Histogram getE2eMsgProcLatencyHistogram() {
        return this.e2eMsgProcLatencyHistogram;
    }

    // end-to-end latency
    private Histogram e2eMsgProcLatencyHistogram;
    private final AmqpBaseOpDispenser s4rBaseOpDispenser;

    public S4RAdapterMetrics(final AmqpBaseOpDispenser s4rBaseOpDispenser, final NBLabeledElement labeledParent) {
        this.s4rBaseOpDispenser = s4rBaseOpDispenser;
        labels=labeledParent.getLabels().and("name", S4RAdapterMetrics.class.getSimpleName());
    }

    public void initS4JAdapterInstrumentation() {
        // Histogram metrics
        messageSizeHistogram =
            ActivityMetrics.histogram(this.s4rBaseOpDispenser,
                "message_size", ActivityMetrics.DEFAULT_HDRDIGITS);

        // Timer metrics
        bindTimer =
            ActivityMetrics.timer(this.s4rBaseOpDispenser,
                    "bind", ActivityMetrics.DEFAULT_HDRDIGITS);
        executeTimer =
            ActivityMetrics.timer(this.s4rBaseOpDispenser,
                     "execute", ActivityMetrics.DEFAULT_HDRDIGITS);

        // End-to-end metrics
        // Latency
        e2eMsgProcLatencyHistogram =
            ActivityMetrics.histogram(this.s4rBaseOpDispenser, "e2e_msg_latency", ActivityMetrics.DEFAULT_HDRDIGITS);
        // Error metrics
        msgErrOutOfSeqCounter =
            ActivityMetrics.counter(this.s4rBaseOpDispenser, "err_msg_oos");
        msgErrLossCounter =
            ActivityMetrics.counter(this.s4rBaseOpDispenser, "err_msg_loss");
        msgErrDuplicateCounter =
            ActivityMetrics.counter(this.s4rBaseOpDispenser, "err_msg_dup");
    }

    public Timer getBindTimer() { return bindTimer; }
    public Timer getExecuteTimer() { return executeTimer; }
    public Histogram getMessagesizeHistogram() { return messageSizeHistogram; }

    public Counter getMsgErrOutOfSeqCounter() {
        return msgErrOutOfSeqCounter;
    }

    public void setMsgErrOutOfSeqCounter(Counter msgErrOutOfSeqCounter) {
        this.msgErrOutOfSeqCounter = msgErrOutOfSeqCounter;
    }

    public Counter getMsgErrLossCounter() {
        return msgErrLossCounter;
    }

    public void setMsgErrLossCounter(Counter msgErrLossCounter) {
        this.msgErrLossCounter = msgErrLossCounter;
    }

    public Counter getMsgErrDuplicateCounter() {
        return msgErrDuplicateCounter;
    }

    public void setMsgErrDuplicateCounter(Counter msgErrDuplicateCounter) {
        this.msgErrDuplicateCounter = msgErrDuplicateCounter;
    }
}
