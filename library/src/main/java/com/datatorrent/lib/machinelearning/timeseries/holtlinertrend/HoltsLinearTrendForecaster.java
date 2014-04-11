package com.datatorrent.lib.machinelearning.timeseries.holtlinertrend;

import com.datatorrent.lib.machinelearning.ModelCreationException;

import java.util.List;

/**
 * Class that implements Holt's Linear Trend Model for time series base forecasting.
 *
 * Forecast equation - yhatt = = lsubt + h * bsubt
 *
 * Level equation - lsubt = α ysubt + (1 − α) (lsubt−1 + bsubt−1)
 *
 * Trend equation - bsubt = β (lsubt − lsubt−1) + (1 − β) bsubt−1
 *
 * References
 * <ul>
 *     <li><a href="http://www.youtube.com/watch?v=e1yUVLKhcko">Mod-02 Lec-03 Forecasting -- Linear Models, Regression, Holts Model</a></li>
 *     <li><a href="https://www.otexts.org/fpp/7/2">Forecasting - Holt's linear trend method</a></li>
 * </ul>
 */
public class HoltsLinearTrendForecaster {

    /**
     * Exponential smoothing constant for Value
     */
    private Double alpha;

    /**
     * Exponential smoothing constant for Slope
     */
    private Double beta;

    private List<Double> data;

    private Double[] lcache;
    private Double[] bcache;

    public HoltsLinearTrendForecaster(double alpha, double beta) {
        this.alpha = alpha;
        this.beta = beta;
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
        if (data == null || data.size() == 0) {
            throw new ModelCreationException("No training data available for initializing the model");
        }

        this.lcache = new Double[future];
        this.bcache = new Double[future];
    }

    public double computeForecast(int future, boolean useRecursion) throws ModelCreationException {
        if (alpha == null || beta == null || data == null) {
            throw new ModelCreationException("Smoothing constants alpha, beta and Time Series data can't be empty!");
        }
        init(future);
        if (future > data.size()) {
            return recursivelyComputeForecast(data.size(), future - data.size());
        }
        return data.get(future);
    }

    /**
     * Compute forecast for the next future period along with step-ahead value. The step-ahead number is simply the period in future.
     *
     * Currently there is only recursive implementation of the algorithm that uses dynamic programming approach to reduce number of recursive calls.
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

        return lsubt + stepAhead * bsubt;
    }

    private double recursivelyComputeLevel(int t) {
        if (t == 0) {
            return 0;
        }
        if (lcache[t] == null) {
            lcache[t] = (alpha * data.get(t - 1)) + ((1 - alpha) * (recursivelyComputeLevel(t - 1) + recursivelyComputeSlope(t - 1)));
        }
        return lcache[t];
    }

    private double recursivelyComputeSlope(int t) {
        if (t == 0) {
            return 0;
        }
        if (bcache[t] == null) {
            bcache[t] = (beta * (recursivelyComputeLevel(t) - recursivelyComputeLevel(t - 1))) + ((1 - beta) * recursivelyComputeSlope(t - 1));
        }
        return bcache[t];
    }
}
