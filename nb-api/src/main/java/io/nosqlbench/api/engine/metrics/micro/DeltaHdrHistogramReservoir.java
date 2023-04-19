package io.nosqlbench.api.engine.metrics.micro;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.Recorder;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.Consumer;
import java.util.function.Function;


public final class DeltaHdrHistogramReservoir implements MicroReservoir {
    private static final Logger logger = LogManager.getLogger(DeltaHdrHistogramReservoir.class);

    // TODO - determine if we want to replace this with micrometer's family of classes as opposed to HdrHistoram
    // For example: AbstractTimeWindowHistogram<T, U> implements Histogram
    // https://github.com/search?q=repo%3Amicrometer-metrics%2Fmicrometer++%22implements+Histogram%22&type=code
    // private final Recorder recorder;

    private Histogram lastHistogram;

    private Histogram intervalHistogram;
    private long intervalHistogramEndTime = System.currentTimeMillis();
    private final String metricName;
    private HistogramLogWriter writer;

    /**
     * Create a reservoir with a default recorder. This recorder should be suitable for most usage.
     *
     * @param name              the name to give to the reservoir, for logging purposes
     * @param significantDigits how many significant digits to track in the reservoir
     */
    public DeltaHdrHistogramReservoir(String name, int significantDigits) {
        this.metricName = name;
        // this.recorder = new Recorder(significantDigits);


        /*
         * Start by flipping the recorder's interval histogram.
         * - it starts our counting at zero. Arguably this might be a bad thing if you wanted to feed in
         *   a recorder that already had some measurements? But that seems crazy.
         * - intervalHistogram can be nonnull.
         * - it lets us figure out the number of significant digits to use in runningTotals.
         */
        intervalHistogram = recorder.getIntervalHistogram();
        lastHistogram = new Histogram(intervalHistogram.getNumberOfSignificantValueDigits());
    }

    public Recorder getRecorder() {
        return this.recorder;
    }

    public Recorder getRecorder() {
        return this.recorder;
    }

    @Override
    public int size() {
        // This appears to be infrequently called, so not keeping a separate counter just for this.
        return getSnapshot().size();
    }

    @Override
    public void update(long value) {
        recorder.recordValue(value);
    }

    /**
     * @return the data accumulated since the reservoir was created, or since the last call to this method
     */
    @Override
    public io.nosqlbench.api.engine.metrics.micro.Snapshot getSnapshot() {
        lastHistogram = getNextHdrHistogram();
        return new io.nosqlbench.api.engine.metrics.micro.DeltaHistogramSnapshot(lastHistogram);
    }

    public Histogram getNextHdrHistogram() {
        return getDataSinceLastSnapshotAndUpdate();
    }


    /**
     * @return last histogram snapshot that was provided by {@link #getSnapshot()}
     */
    public Snapshot getLastSnapshot() {
        return new DeltaHistogramSnapshot(lastHistogram);
    }

    /**
     * @return a copy of the accumulated state since the reservoir last had a snapshot
     */
    private synchronized Histogram getDataSinceLastSnapshotAndUpdate() {
        intervalHistogram = recorder.getIntervalHistogram(intervalHistogram);
        long intervalHistogramStartTime = intervalHistogramEndTime;
        intervalHistogramEndTime = System.currentTimeMillis();

        intervalHistogram.setTag(metricName);
        intervalHistogram.setStartTimeStamp(intervalHistogramStartTime);
        intervalHistogram.setEndTimeStamp(intervalHistogramEndTime);

        lastHistogram = intervalHistogram.copy();
        lastHistogram.setTag(metricName);

        if (writer != null) {
            writer.outputIntervalHistogram(lastHistogram);
        }
        return lastHistogram;
    }

    /**
     * Write the last results via the log writer.
     *
     * @param writer the log writer to use
     */
    public void write(HistogramLogWriter writer) {
        writer.outputIntervalHistogram(lastHistogram);
    }

    public io.nosqlbench.api.engine.metrics.micro.DeltaHdrHistogramReservoir copySettings() {
        return new io.nosqlbench.api.engine.metrics.micro.DeltaHdrHistogramReservoir(this.metricName,
                intervalHistogram.getNumberOfSignificantValueDigits());
    }

    public void attachLogWriter(HistogramLogWriter logWriter) {
        this.writer = logWriter;
    }

    public Histogram getLastHistogram() {
        return lastHistogram;
    }

    /**
     * Summary statistics should be published off of a single snapshot instance so that,
     * for example, there isn't disagreement between the distribution's bucket counts
     * because more events continue to stream in.
     *
     * @return A snapshot of all distribution statistics at a point in time.
     */
    @Override
    public HistogramSnapshot takeSnapshot() {
        throw new NotImplementedException("TODO impl");
    }

    /**
     * @return A unique combination of name and tags
     */
    @Override
    public Id getId() {
        throw new NotImplementedException("TODO impl");
    }

    /**
     * Get a set of measurements. Should always return the same number of measurements and
     * in the same order, regardless of the level of activity or the lack thereof.
     *
     * @return The set of measurements that represents the instantaneous value of this
     * meter.
     */
    @Override
    public Iterable<Measurement> measure() {
        throw new NotImplementedException("TODO impl");
    }

    /**
     * Match a {@link Meter} by type with series of dedicated functions for specific
     * {@link Meter}s and return a result from the matched function.
     * <p>
     * NOTE: This method contract will change in minor releases if ever a new
     * {@link Meter} type is created. In this case only, this is considered a feature. By
     * using this method, you are declaring that you want to be sure to handle all types
     * of meters. A breaking API change during the introduction of a new {@link Meter}
     * indicates that there is a new meter type for you to consider and the compiler will
     * effectively require you to consider it.
     *
     * @param visitGauge           function to apply for {@link Gauge}
     * @param visitCounter         function to apply for {@link Counter}
     * @param visitTimer           function to apply for {@link Timer}
     * @param visitSummary         function to apply for {@link DistributionSummary}
     * @param visitLongTaskTimer   function to apply for {@link LongTaskTimer}
     * @param visitTimeGauge       function to apply for {@link TimeGauge}
     * @param visitFunctionCounter function to apply for {@link FunctionCounter}
     * @param visitFunctionTimer   function to apply for {@link FunctionTimer}
     * @param visitMeter           function to apply as a fallback
     * @return return value from the applied function
     * @since 1.1.0
     */
    @Override
    public <T> T match(Function<Gauge, T> visitGauge, Function<Counter, T> visitCounter, Function<Timer, T> visitTimer,
                       Function<DistributionSummary, T> visitSummary, Function<LongTaskTimer, T> visitLongTaskTimer,
                       Function<TimeGauge, T> visitTimeGauge, Function<FunctionCounter, T> visitFunctionCounter,
                       Function<FunctionTimer, T> visitFunctionTimer, Function<Meter, T> visitMeter) {
        return MicroReservoir.super.match(visitGauge, visitCounter, visitTimer, visitSummary,
                visitLongTaskTimer, visitTimeGauge, visitFunctionCounter, visitFunctionTimer, visitMeter);
    }

    /**
     * Match a {@link Meter} with a series of dedicated functions for specific
     * {@link Meter}s and call the matching consumer.
     * <p>
     * NOTE: This method contract will change in minor releases if ever a new
     * {@link Meter} type is created. In this case only, this is considered a feature. By
     * using this method, you are declaring that you want to be sure to handle all types
     * of meters. A breaking API change during the introduction of a new {@link Meter}
     * indicates that there is a new meter type for you to consider and the compiler will
     * effectively require you to consider it.
     *
     * @param visitGauge           function to apply for {@link Gauge}
     * @param visitCounter         function to apply for {@link Counter}
     * @param visitTimer           function to apply for {@link Timer}
     * @param visitSummary         function to apply for {@link DistributionSummary}
     * @param visitLongTaskTimer   function to apply for {@link LongTaskTimer}
     * @param visitTimeGauge       function to apply for {@link TimeGauge}
     * @param visitFunctionCounter function to apply for {@link FunctionCounter}
     * @param visitFunctionTimer   function to apply for {@link FunctionTimer}
     * @param visitMeter           function to apply as a fallback
     * @since 1.1.0
     */
    @Override
    public void use(Consumer<Gauge> visitGauge, Consumer<Counter> visitCounter, Consumer<Timer> visitTimer,
                    Consumer<DistributionSummary> visitSummary, Consumer<LongTaskTimer> visitLongTaskTimer,
                    Consumer<TimeGauge> visitTimeGauge, Consumer<FunctionCounter> visitFunctionCounter,
                    Consumer<FunctionTimer> visitFunctionTimer, Consumer<Meter> visitMeter) {
        MicroReservoir.super.use(visitGauge, visitCounter, visitTimer, visitSummary, visitLongTaskTimer, visitTimeGauge, visitFunctionCounter, visitFunctionTimer, visitMeter);
    }

    @Override
    public void close() {
        MicroReservoir.super.close();
    }
}