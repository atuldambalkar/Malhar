package com.datatorrent.lib.machinelearning.timeseries.simpleexponentialaverage;

import com.datatorrent.lib.machinelearning.timeseries.TimeSeriesData;

import java.util.List;

/**
 * Class to handle Simple/Single Exponential Average Smoothing based time-series forecasting.
 *
 * Class performs the Simple/Single Exponential Average based Smoothing. Following is the equation used.
 *
 * FsubTplusOne = alpha * DsubT + ((1 - alpha) * FsubT
 * where,
 *     alpha = Smoothing constant 0 < alpha < 1
 *     FsubTplusOne = Forecast for Time Period T + 1
 *     DsubT = Demand for Time Period T
 *     FsubT = Forecast for Time Period T
 *
 * As seen by above equation, this forecasting method expects training data or demand data to be available till
 * last time period (T) for predicting the value for T+1 time period.
 *
 * Reference links -
 * <ul>
 *   <li>http://www.youtube.com/watch?v=k9dhcfIyOFc</li>
 *   <li>Lecture Series from Prof. G. Srinivasn, IIT Madras, Forecasting -- Time series models -- Simple Exponential smoothing</li>
 * </ul>
 */
public class SEASmoothingForecaster {

    private List<TimeSeriesData> timeSeriesDataList;

    /**
     * Smoothing constant to use. The value should be between 1 and 0.
     *
     * If the value of alpha is higher, then the forecast tend to use demand as the basis for forecast.
     */
    private Double alpha;

    public SEASmoothingForecaster() {
    }

    public SEASmoothingForecaster(List<TimeSeriesData> timeSeriesDataList, double alpha) {
        this.timeSeriesDataList = timeSeriesDataList;
        this.alpha = alpha;
    }

    public void setTimeSeriesDataList(List<TimeSeriesData> timeSeriesDataList) {
        this.timeSeriesDataList = timeSeriesDataList;
    }

    public void setAlpha(double alpha) {
        this.alpha = alpha;
    }

    /**
     *
     * @param useRecursion Use recursion to compute the forecast value. Else iterative approach will be used.
     * @return
     */
    public double computeForecast(boolean useRecursion) {
        if (alpha == null || timeSeriesDataList == null) {
            throw new IllegalArgumentException("Smoothing constant and Time Series data can't be empty!");
        }
        return useRecursion?
                recursivelyComputeForecast(timeSeriesDataList.size()):
                iterativelyComputeForecast(timeSeriesDataList.size());
    }

    private double recursivelyComputeForecast(int tplusOne) {
        if (tplusOne == 0) {
            return 0;
        }
        int t = tplusOne - 1;
        return (alpha * timeSeriesDataList.get(t).y) + ((1 - alpha) * recursivelyComputeForecast(t));
    }

    private double iterativelyComputeForecast(int period) {
        double forecast = 0;
        for (int i = 0; i < period; i++) {
            forecast = computeLastForecast(i, forecast);
        }
        return forecast;
    }

    private double computeLastForecast(int period, double lastForecast) {
        return (alpha * timeSeriesDataList.get(period).y) + ((1 - alpha) * lastForecast);
    }
}
