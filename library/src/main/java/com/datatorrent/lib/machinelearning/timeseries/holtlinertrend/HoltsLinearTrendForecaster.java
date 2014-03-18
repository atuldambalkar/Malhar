package com.datatorrent.lib.machinelearning.timeseries.holtlinertrend;

import com.datatorrent.lib.machinelearning.ModelNotReadyException;

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

    public HoltsLinearTrendForecaster(double alpha, double beta) {
        this.alpha = alpha;
        this.beta = beta;
    }

    public void setTimeSeriesData(List<Double> data) {
        this.data = data;
    }

    public double computeForecast(int future, boolean useRecursion) throws ModelNotReadyException {
        if (alpha == null || beta == null || data == null) {
            throw new ModelNotReadyException("Smoothing constants alpha, beta and Time Series data can't be empty!");
        }
        if (future > data.size()) {
            return recursivelyComputeForecast(data.size(), (future - (data.size() - 1)) == 0? 1: future - (data.size() - 1));
        }
        return data.get(future);
    }

    private double recursivelyComputeForecast(int tplusOne, int stepAhead) {
        if (tplusOne == 0) {
            return 0;
        }
        int t = tplusOne - 1;
        double lsubt = recursivelyComputeValue(t);
        double bsubt = recursivelyComputeSlope(t);

        return lsubt + stepAhead * bsubt;
    }

    private double recursivelyComputeValue(int t) {
        if (t == 0) {
            return 0;
        }
        return (alpha * data.get(t)) + ((1 - alpha) * (recursivelyComputeValue(t - 1) + recursivelyComputeSlope(t - 1)));
    }

    private double recursivelyComputeSlope(int t) {
        if (t == 0) {
            return 0;
        }
        return (beta * (recursivelyComputeValue(t) - recursivelyComputeValue(t - 1))) + ((1 - beta) * recursivelyComputeSlope(t - 1));
    }

//    private double iterativelyComputeForecast(int period) {
//        double forecast = 0;
//        for (int i = 0; i < period; i++) {
//            forecast = computeLastForecast(i, forecast);
//        }
//        return forecast;
//    }
//
//    private double computeLastForecast(int period, double lastForecast) {
//        return (alpha * data.get(period)) + ((1 - alpha) * lastForecast);
//    }
}
