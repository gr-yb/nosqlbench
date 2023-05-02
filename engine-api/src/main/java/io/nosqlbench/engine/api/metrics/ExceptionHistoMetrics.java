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

package io.nosqlbench.engine.api.metrics;

import com.codahale.metrics.Histogram;
import io.nosqlbench.api.config.NBLabeledElement;
import io.nosqlbench.api.engine.activityimpl.ActivityDef;
import io.nosqlbench.api.engine.metrics.ActivityMetrics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Use this to provide exception histograms in a uniform way.
 * To use this, you need to have a way to get a meaningful magnitude
 * from each type of error you want to track.
 */
public class ExceptionHistoMetrics {
    private final ConcurrentHashMap<String, Histogram> histos = new ConcurrentHashMap<>();
    private final Histogram allerrors;
    private final NBLabeledElement parentLabels;
    private final ActivityDef activityDef;

    public ExceptionHistoMetrics(final NBLabeledElement parentLabels, final ActivityDef activityDef) {
        this.parentLabels = parentLabels;
        this.activityDef = activityDef;
        this.allerrors = ActivityMetrics.histogram(parentLabels, "errorhistos.ALL", activityDef.getParams().getOptionalInteger("hdr_digits").orElse(4));
    }

    public void update(final String name, final long magnitude) {
        Histogram h = this.histos.get(name);
        if (null == h) synchronized (this.histos) {
            h = this.histos.computeIfAbsent(
                name,
                k -> ActivityMetrics.histogram(this.parentLabels, "errorhistos." + name, this.activityDef.getParams().getOptionalInteger("hdr_digits").orElse(4))
            );
        }
        h.update(magnitude);
        this.allerrors.update(magnitude);
    }


    public List<Histogram> getHistograms() {
        return new ArrayList<>(this.histos.values());
    }
}
