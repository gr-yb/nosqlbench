package io.nosqlbench.api.engine.metrics.micro;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import org.HdrHistogram.Histogram;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.*;


public final class NicerTimer extends MicroObservable {

    private final DeltaHdrHistogramReservoir deltaHdrHistogramReservoir;
    private long cacheExpiry = 0L;
    private List<Timer> mirrors;

    private String metricName;


    public NicerTimer(String metricName, DeltaHdrHistogramReservoir deltaHdrHistogramReservoir) {
        this.metricName = metricName;
        this.deltaHdrHistogramReservoir = deltaHdrHistogramReservoir;
    }


    public ConvenientSnapshot getSnapshot() {
        if (System.currentTimeMillis() >= cacheExpiry) {
            return new ConvenientSnapshot(deltaHdrHistogramReservoir.getSnapshot());
        } else {
            return new ConvenientSnapshot(deltaHdrHistogramReservoir.getLastSnapshot());
        }
    }

    public io.nosqlbench.api.engine.metrics.micro.DeltaSnapshotReader getDeltaReader() {
        return new io.nosqlbench.api.engine.metrics.micro.DeltaSnapshotReader(this);
    }

    @Override
    public io.nosqlbench.api.engine.metrics.micro.ConvenientSnapshot getDeltaSnapshot(long cacheTimeMillis) {
        this.cacheExpiry = System.currentTimeMillis() + cacheTimeMillis;
        return new io.nosqlbench.api.engine.metrics.micro.ConvenientSnapshot(deltaHdrHistogramReservoir.getSnapshot());
    }

    @Override
    public synchronized io.nosqlbench.api.engine.metrics.micro.NicerTimer attachHdrDeltaHistogram() {
        if (mirrors == null) {
            mirrors = new CopyOnWriteArrayList<>();
        }
        DeltaHdrHistogramReservoir sameConfigReservoir = this.deltaHdrHistogramReservoir.copySettings();
        io.nosqlbench.api.engine.metrics.micro.NicerTimer mirror =
                new io.nosqlbench.api.engine.metrics.micro.NicerTimer(this.metricName, sameConfigReservoir);
        mirrors.add(mirror);
        return mirror;
    }

    @Override
    public Timer attachTimer(Timer timer) {
        if (mirrors == null) {
            mirrors = new CopyOnWriteArrayList<>();
        }
        mirrors.add(timer);
        return timer;
    }


    @Override
    public Histogram getNextHdrDeltaHistogram() {
        return this.deltaHdrHistogramReservoir.getNextHdrHistogram();
    }


    @Override
    public void record(long amount, TimeUnit unit) {
        deltaHdrHistogramReservoir.getRecorder().recordValue(amount);
    }

    /**
     * Updates the statistics kept by the timer with the specified amount.
     *
     * @param duration Duration of a single event being measured by this timer.
     */
    @Override
    public void record(Duration duration) {
        super.record(duration);
    }

    @Override
    public <T> T record(Supplier<T> f) {
        return null;
    }

    /**
     * Executes the Supplier {@code f} and records the time taken.
     *
     * @param f Function to execute and measure the execution time.
     * @return The return value of {@code f}.
     * @since 1.10.0
     */
    @Override
    public boolean record(BooleanSupplier f) {
        return super.record(f);
    }

    /**
     * Executes the Supplier {@code f} and records the time taken.
     *
     * @param f Function to execute and measure the execution time.
     * @return The return value of {@code f}.
     * @since 1.10.0
     */
    @Override
    public int record(IntSupplier f) {
        return super.record(f);
    }

    /**
     * Executes the Supplier {@code f} and records the time taken.
     *
     * @param f Function to execute and measure the execution time.
     * @return The return value of {@code f}.
     * @since 1.10.0
     */
    @Override
    public long record(LongSupplier f) {
        return super.record(f);
    }

    /**
     * Executes the Supplier {@code f} and records the time taken.
     *
     * @param f Function to execute and measure the execution time.
     * @return The return value of {@code f}.
     * @since 1.10.0
     */
    @Override
    public double record(DoubleSupplier f) {
        return super.record(f);
    }

    @Override
    public <T> T recordCallable(Callable<T> f) throws Exception {
        return null;
    }

    @Override
    public void record(Runnable f) {

    }

    /**
     * Wrap a {@link Runnable} so that it is timed when invoked.
     *
     * @param f The Runnable to time when it is invoked.
     * @return The wrapped Runnable.
     */
    @Override
    public Runnable wrap(Runnable f) {
        return super.wrap(f);
    }

    /**
     * Wrap a {@link Callable} so that it is timed when invoked.
     *
     * @param f The Callable to time when it is invoked.
     * @return The wrapped callable.
     */
    @Override
    public <T> Callable<T> wrap(Callable<T> f) {
        return super.wrap(f);
    }

    /**
     * Wrap a {@link Supplier} so that it is timed when invoked.
     *
     * @param f The {@code Supplier} to time when it is invoked.
     * @return The wrapped supplier.
     * @since 1.2.0
     */
    @Override
    public <T> Supplier<T> wrap(Supplier<T> f) {
        return super.wrap(f);
    }

    @Override
    public long count() {
        return 0;
    }

    @Override
    public double totalTime(TimeUnit unit) {
        return 0;
    }

    /**
     * @param unit The base unit of time to scale the mean to.
     * @return The distribution average for all recorded events.
     */
    @Override
    public double mean(TimeUnit unit) {
        return super.mean(unit);
    }

    @Override
    public double max(TimeUnit unit) {
        return 0;
    }

    /**
     * @return A unique combination of name and tags
     */
    @Override
    public Id getId() {
        return null;
    }

    @Override
    public Iterable<Measurement> measure() {
        return null;
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
    public <T> T match(Function<Gauge, T> visitGauge, Function<Counter, T> visitCounter, Function<Timer, T> visitTimer, Function<DistributionSummary, T> visitSummary, Function<LongTaskTimer, T> visitLongTaskTimer, Function<TimeGauge, T> visitTimeGauge, Function<FunctionCounter, T> visitFunctionCounter, Function<FunctionTimer, T> visitFunctionTimer, Function<Meter, T> visitMeter) {
        return super.match(visitGauge, visitCounter, visitTimer, visitSummary, visitLongTaskTimer, visitTimeGauge, visitFunctionCounter, visitFunctionTimer, visitMeter);
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
        super.use(visitGauge, visitCounter, visitTimer, visitSummary, visitLongTaskTimer, visitTimeGauge,
                visitFunctionCounter, visitFunctionTimer, visitMeter);
    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    public TimeUnit baseTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }

    @Override
    public HistogramSnapshot takeSnapshot() {
        return deltaHdrHistogramReservoir.takeSnapshot();
    }


    public void update(long duration, TimeUnit unit) {
        deltaHdrHistogramReservoir.update(duration);
        if (mirrors != null) {
            for (Timer mirror : mirrors) {
                mirror.record(duration, unit);
            }
        }
    }


}
