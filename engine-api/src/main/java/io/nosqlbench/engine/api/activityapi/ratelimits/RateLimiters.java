/*
 * Copyright (c) 2022 nosqlbench
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

package io.nosqlbench.engine.api.activityapi.ratelimits;

import com.codahale.metrics.Gauge;
import io.nosqlbench.api.config.NBNamedElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RateLimiters {
    private final static Logger logger = LogManager.getLogger(RateLimiters.class);

    private final static ThreadLocal<RateLimiter> tlrl = new ThreadLocal<>();

    /**
     * Allow for a thread-local rate limiter to be created. Only one namespace is supported here.
     * @param def The NBNamedElement, for naming and identification
     * @param label a label to be applied to the rate limiter within the named element
     * @param spec the specifier for the rate limiter
     * @return a new or modified rate limiter
     */
    public static synchronized RateLimiter createOrUpdateThreadLocal(NBNamedElement def, String label, RateSpec spec) {

        RateLimiter rateLimiter = tlrl.get();

        if (rateLimiter==null) {
            rateLimiter = new HybridRateLimiter(def, label+":"+Thread.currentThread().getName(), spec);
            logger.info("Using thread-local rate limiter: " + rateLimiter);
            tlrl.set(rateLimiter);
        } else {
            rateLimiter.applyRateSpec(spec);
            logger.info("updated thread-local rate limiter: " + rateLimiter);
        }
        return rateLimiter;
    }

    public static synchronized RateLimiter createOrUpdate(NBNamedElement def, String label, RateLimiter extant, RateSpec spec) {

        if (extant == null) {
            RateLimiter rateLimiter= new HybridRateLimiter(def, label, spec);

            logger.info("Using rate limiter: " + rateLimiter);
            return rateLimiter;
        } else {
            extant.applyRateSpec(spec);
            logger.info("Updated rate limiter: " + extant);
            return extant;
        }
    }

    public static synchronized RateLimiter create(NBNamedElement def, String label, String specString) {
        return createOrUpdate(def, label, null, new RateSpec(specString));
    }

    public static class WaitTimeGauge implements Gauge<Long> {

        private final RateLimiter rateLimiter;

        public WaitTimeGauge(RateLimiter rateLimiter) {
            this.rateLimiter = rateLimiter;
        }

        @Override
        public Long getValue() {
            return rateLimiter.getTotalWaitTime();
        }
    }

    public static class RateGauge implements Gauge<Double> {
        private final RateLimiter rateLimiter;

        public RateGauge(RateLimiter rateLimiter) {
            this.rateLimiter = rateLimiter;
        }

        @Override
        public Double getValue() {
            return rateLimiter.getRateSpec().opsPerSec;
        }
    }

    public static class BurstRateGauge implements Gauge<Double> {
        private final RateLimiter rateLimiter;

        public BurstRateGauge(RateLimiter rateLimiter) {
            this.rateLimiter = rateLimiter;
        }

        @Override
        public Double getValue() {
            return rateLimiter.getRateSpec().getBurstRatio() * rateLimiter.getRateSpec().getRate();
        }
    }

}
