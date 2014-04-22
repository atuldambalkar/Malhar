package com.datatorrent.lib.machinelearning.timeseries.holtwintersseasonal;

import com.datatorrent.lib.machinelearning.ModelCreationException;

import java.util.List;

/**
 * Class that implements Holt Winters' Seasonal Forecasting with Multiplicative Method Model for time series base forecasting.
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
public class HoltWintersSeasonalMultiplicativeForecaster {

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

    private List<Double> data;

    public HoltWintersSeasonalMultiplicativeForecaster(int numPeriods, double alpha, double beta, double gamma) throws ModelCreationException {
        this.numPeriods = numPeriods;
        this.alpha = alpha;
        this.beta = beta;
        this.gamma = gamma;
        this.sinit = new double[numPeriods];
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
        this.scache = new Double[future];

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
     * Currently there is only recursive implementation of the algorithm that uses dynamic programming approach to reduce number of recursive calls.
     *
     * @param future
     * @param useRecursion
     * @return
     * @throws ModelCreationException
     */
    public double computeForecast(int future, boolean useRecursion) throws ModelCreationException {
        if (alpha == null || beta == null || gamma == null || data == null) {
            throw new ModelCreationException("Smoothing constants alpha, beta, gamma and Time Series data can't be empty!");
        }
        init(future);
        if (future > data.size()) {
            return recursivelyComputeForecast(data.size(), future - data.size());
        }
        return data.get(future);
    }

    /**
     * Forecast equation - yhatt+h = (lsubt-1 + h * bsubt-1) * ssubt-m+hsubmplus
     *
     * @param t
     * @param stepAhead
     * @return
     */
    private double recursivelyComputeForecast(int t, int stepAhead) {
        if (t == 0) {
            return 0;
        }

        double lsubt = recursivelyComputeLevel(t);
        double bsubt = recursivelyComputeSlope(t);
        double ssubtmhsubmplus = recursivelyComputeSeasonality(t - numPeriods + getHsubMplus(stepAhead));

        return (lsubt + stepAhead * bsubt) * ssubtmhsubmplus;
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
    private double recursivelyComputeLevel(int t) {
        if (t == 0) {
            return 0;
        }
        if (lcache[t] == null) {
            lcache[t] = (alpha * (data.get(t - 1) / recursivelyComputeSeasonality(t - numPeriods))) + ((1 - alpha) * (recursivelyComputeLevel(t - 1) + recursivelyComputeSlope(t - 1)));
        }
        return lcache[t];
    }

    /**
     * Trend equation - bsubt = β (lsubt − lsubt−1) + (1 − β) bsubt−1
     *
     * @param t
     * @return
     */
    private double recursivelyComputeSlope(int t) {
        if (t == 0) {
            return 0;
        }
        if (bcache[t] == null) {
            bcache[t] = (beta * (recursivelyComputeLevel(t) - recursivelyComputeLevel(t - 1))) + ((1 - beta) * recursivelyComputeSlope(t - 1));
        }
        return bcache[t];
    }

    /**
     * Seasonality equation - ssubt = γ (ysubt / (lsubt + bsubt-1)) + (1 - γ) ssubt-m
     *
     * @param t
     * @return
     */
    private double recursivelyComputeSeasonality(int t) {
        if (t == 0) {
            return 0;
        }
        if (scache[t] == null) {
            scache[t] = (gamma * (data.get(t - 1) / (recursivelyComputeLevel(t - 1) + recursivelyComputeSlope(t - 1)))) + ((1 - gamma) * recursivelyComputeSeasonality(t - numPeriods));
        }
        return scache[t];
    }
}
