package com.datatorrent.lib.machinelearning.timeseries.holtwintersseasonal;

import com.datatorrent.lib.machinelearning.ForecastingException;
import com.datatorrent.lib.machinelearning.ModelCreationException;
import com.datatorrent.lib.machinelearning.ModelUpdationException;
import org.apache.commons.collections.buffer.CircularFifoBuffer;

import java.util.List;

/**
 * Class that implements Holt Winters' Seasonal Forecasting with Multiplicative Method Model for time series base forecasting with incremental update of the model.
 *
 * For any given input data, in order to calculate Level - lsubt, Trend - bsubt the values for previous records for last one time-series period
 * are needed. The seasonal values cached are for two time series cycles periods earlier.
 *
 * After building the model based on the initial training data, this class only keeps track of the
 * previous values for Level, Trend and Seasonality and uses those values to update the new Level, Trend and Seasonality values.
 *
 * Forecast equation - yhatt+h = (lsubt-1 + h * bsubt-1) * ssubt-m+hsubmplus
 *     where hsubmpplus = floor ((h−1)mod m) + 1
 *
 * Level equation - lsubt = α (ysubt / ssubt-m) + (1 − α) (lsubt−1 + bsubt−1)
 *
 * Trend equation - bsubt = β (lsubt − lsubt−1) + (1 − β) bsubt−1
 *
 * Seasonality equation - ssubt = γ (ysubt / (lsubt + bsubt-1)) + (1 - γ) ssubt-m
 *
 * h stands for - step ahead
 * m stands for - number of periods in the time cycle
 *
 * References
 * <ul>
 *     <li><a href="https://www.otexts.org/fpp/7/5">Holt-Winters seasonal multiplicative method</a></li>
 *     <li><a href="https://www.youtube.com/watch?v=MbNmIZNy3qI">Mod-02 Lec-04 Forecasting -- Winter's model, causal models, Goodness of forecast, Aggregate Planning</a></li>
 * </ul>
 */
public class HoltWintersSeasonalMultiplicativeIncrementalForecaster {

    /**
     * Number of periods in the time cycle
     */
    private int numPeriods;

    /**
     * Exponential smoothing constant for Value
     */
    private Double alpha;

    /**
     * Exponential smoothing constant for Slope
     */
    private Double beta;

    /**
     * Exponential smoothing constant for Seasonality
     */
    private Double gamma;

    /**
     * Initialized value for level based on the the first cycle values.
     */
    private double linit;

    /**
     * Initialized value for trend based on the the first cycle values
     */
    private double binit;

    /**
     * Initialized values for seasonality based on the the first cycle values
     */
    private double[] sinit;

    private Double[] lcache;
    private Double[] bcache;
    private Double[] scache;
    private boolean modelReady;

    private List<Double> data;

    private int dataSize;

    /**
     * The time period across time-series cycles for which data is received and processed to build the model.
     * For example, if so far algorithm has processed 6 time-series cycles worth of data each with 12 time period, then the value
     * for this variable will be 72. As subsequent data values are received this value should be incremented and that happens in
     * incrementModel method.
     */
    private int currentTimePeriod;

    private CircularFifoBuffer dataCache;
    private CircularFifoBuffer prevLevelCache;
    private CircularFifoBuffer prevTrendCache;
    private CircularFifoBuffer prevSeasonalityCache;

    public HoltWintersSeasonalMultiplicativeIncrementalForecaster(int numPeriods, double alpha, double beta, double gamma) throws ModelCreationException {
        this.numPeriods = numPeriods;
        this.alpha = alpha;
        this.beta = beta;
        this.gamma = gamma;
        this.sinit = new double[numPeriods];
        this.dataCache = new CircularFifoBuffer(numPeriods);
        this.prevLevelCache = new CircularFifoBuffer(numPeriods + 1);
        this.prevTrendCache = new CircularFifoBuffer(numPeriods + 1);
        this.prevSeasonalityCache = new CircularFifoBuffer(numPeriods);
    }

    public void setTimeSeriesData(List<Double> data) {
        this.data = data;
    }

    /**
     * Initialize the model by computing values for first cycle in the given training data.
     *
     * level = average of all the values in first cycle.
     * trend = (last value in cycle - first value in cycle) / number of periods
     * seasonality = value in cycle / calculated level
     */
    private void init(int future) throws ModelCreationException {
        if (data == null || data.size() == 0 || data.size() < numPeriods) {
            throw new ModelCreationException("No training data available for initializing the model");
        }

        linit = 0;
        binit = 0;
        sinit = new double[numPeriods];

        this.lcache = new Double[future];
        this.bcache = new Double[future];
        this.scache = new Double[future + 1];

        // calculate level value
        for (int i = 0; i < numPeriods; i++) {
            linit += data.get(i);
        }
        linit /= numPeriods;
        for (int i = 1; i <= numPeriods; i++) {
            lcache[i] = linit;
        }

        // calculate trend value
        binit = (data.get(numPeriods - 1) - data.get(0)) / (numPeriods - 1);
        for (int i = 1; i <= numPeriods; i++) {
            bcache[i] = binit;
        }

        // calculate seasonality
        for (int i = 0; i < numPeriods; i++) {
            sinit[i] = data.get(i) / linit;
        }
        for (int i = 1; i <= numPeriods; i++) {
            scache[i] = sinit[i - 1];
        }

    }


    /**
     * Initialize the model by computing values for first cycle in the given training data.
     *
     * level = average of all the values in first cycle.
     * trend = (last value in cycle - first value in cycle) / number of periods
     * seasonality = value in cycle / calculated level
     */
    public void buildModel() throws ModelCreationException {
        if (data == null || data.size() == 0 || data.size() < numPeriods) {
            throw new ModelCreationException("No training data available for initializing the model");
        }
        if (data.size() % numPeriods != 0) {
            throw new ModelCreationException("Insufficient training data. Please provide training data for all the time periods for all the time-series cycles.");
        }

        int size = data.size();
        this.dataSize = size;

        init(size + 1);   // future is just 1 step away

        buildModel(size);

        currentTimePeriod = size + 1;
        modelReady = true;
        lcache = null;
        bcache = null;
        scache = null;
    }

    /**
     * Currently there is only recursive implementation of the algorithm that uses dynamic programming approach to reduce number of recursive calls.
     *
     * @return
     * @throws com.datatorrent.lib.machinelearning.ModelCreationException
     */
    private void buildModel(int t) throws ModelCreationException {
        if (alpha == null || beta == null || gamma == null || data == null) {
            throw new ModelCreationException("Smoothing constants alpha, beta, gamma and Time Series data can't be empty!");
        }

        double lsubt = buildLevel(t);
        double bsubt = buildTrend(t);
        double ssubt = buildSeasonality(t);

        if (lcache.length != dataSize + 1) {
            throw new ModelCreationException("Exception while creating model. Insufficient number of model elements for Level computations");
        }
        for (int i = dataSize - numPeriods; i <= dataSize; i++) {
            dataCache.add(data.get(i - 1));
        }
        // copy the data cache and computed values for only last time-series cycle into circular buffer
        for (int i = dataSize - numPeriods; i <= dataSize; i++) {
            prevLevelCache.add(lcache[i]);
            prevTrendCache.add(bcache[i]);
        }
        for (int i = dataSize - (2 * numPeriods) + 1; i <= dataSize - numPeriods; i++) {
            prevSeasonalityCache.add(scache[i]);
        }
    }

    /**
     * hsubmplus = floor ((h−1)mod m) + 1
     *
     * @param stepAhead
     * @return
     */
    private int getHsubMplus(int stepAhead) {
        return ((stepAhead - 1) % numPeriods) + 1;
    }

    /**
     * Level equation - lsubt = α (ysubt / ssubt-m) + (1 − α) (lsubt−1 + bsubt−1)
     *
     * @param t
     * @return
     */
    private double buildLevel(int t) {
        if (t == 0) {
            return 0;
        }
        if (lcache[t] == null) {
            lcache[t] = (alpha * (data.get(t - 1) / buildSeasonality(t - numPeriods))) + ((1 - alpha) * (buildLevel(t - 1) + buildTrend(t - 1)));
        }
        return lcache[t];
    }

    /**
     * Trend equation - bsubt = β (lsubt − lsubt−1) + (1 − β) bsubt−1
     *
     * @param t
     * @return
     */
    private double buildTrend(int t) {
        if (t == 0) {
            return 0;
        }
        if (bcache[t] == null) {
            bcache[t] = (beta * (buildLevel(t) - buildLevel(t - 1))) + ((1 - beta) * buildTrend(t - 1));
        }
        return bcache[t];
    }

    /**
     * Seasonality equation - ssubt = γ (ysubt / (lsubt + bsubt-1)) + (1 - γ) ssubt-m
     *
     * @param t
     * @return
     */
    private double buildSeasonality(int t) {
        if (t == 0) {
            return 0;
        }
        if (scache[t] == null) {
            scache[t] = (gamma * (data.get(t - 1) / (buildLevel(t - 1) + buildTrend(t - 1)))) + ((1 - gamma) * buildSeasonality(t - numPeriods));
        }
        return scache[t];
    }

    /**
     * Use the given data to incrementally update the model.
     * @param data
     */
    public void incrementModel(double data) throws ModelUpdationException {
        if (!modelReady) {
            throw new ModelUpdationException("Model seems to be empty, hence can not be updated!");
        }

        Object[] lcache = prevLevelCache.toArray();
        Object[] bcache = prevTrendCache.toArray();
        Object[] scache = prevSeasonalityCache.toArray();
        Object dcache = dataCache.remove();
        dataCache.add(data);
        double prevLvalue = (Double)prevLevelCache.remove();
        double prevBvalue = (Double)prevTrendCache.remove();
        double prevSvalue = (Double)prevSeasonalityCache.remove();

        double svalue = incrementSeasonality((Double)dcache, prevLvalue, prevBvalue, prevSvalue);

        prevLvalue = (Double)prevLevelCache.toArray()[numPeriods - 1];
        prevBvalue = (Double)prevTrendCache.toArray()[numPeriods - 1];

        double lvalue = (alpha * (data / svalue)) + ((1 - alpha) * (prevLvalue + prevBvalue));
        double bvalue = (beta * (lvalue - prevLvalue)) + ((1 - beta) * prevBvalue);

        prevLevelCache.add(lvalue);
        prevTrendCache.add(bvalue);
        prevSeasonalityCache.add(svalue);

        // now increment the currentTimePeriod
        this.currentTimePeriod++;
    }

    public double incrementSeasonality(double data, double level, double trend, double seasonality) {
        return (gamma * (data / (level + trend))) + ((1 - gamma) * seasonality);
    }


    /**
     *
     * @param future
     * @return
     */
    public double computeForecast(int future) throws ForecastingException {
        if (future < dataSize) {
            throw new ForecastingException("Can't forecast value for past period!");
        }
        int stepAhead = future - (currentTimePeriod - 1);
        int lastSeasonalityPeriod = ((currentTimePeriod - 1 - numPeriods) % numPeriods) + getHsubMplus(stepAhead);

        Object[] datacache = dataCache.toArray();
        Object[] prevlcache = prevLevelCache.toArray();
        Object[] prevbcache = prevTrendCache.toArray();
        Object[] prevscache = prevSeasonalityCache.toArray();

        double svalue = incrementSeasonality((Double)datacache[lastSeasonalityPeriod - 1],
                (Double)prevlcache[lastSeasonalityPeriod - 1],
                (Double)prevbcache[lastSeasonalityPeriod - 1],
                (Double)prevscache[lastSeasonalityPeriod - 1]);

        return ((Double)prevlcache[numPeriods] + stepAhead * (Double)prevbcache[numPeriods]) * svalue;
    }
}
