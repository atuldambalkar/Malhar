package com.datatorrent.lib.machinelearning.timeseries.simpleexponentialaverage;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.lib.machinelearning.timeseries.TimeSeriesData;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;

/**
 * Operator to handle Simple/Single Exponential Average Smoothing based time-series forecasting.
 *
 * Operator performs the Simple/Single Exponential Average based Smoothing. Following is the equation used.
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
 * This operator forecasts the value for the next time period in endWindow API.
 */
@OperatorAnnotation (partitionable = false)
public class SEASmoothingForecastingOperator extends BaseOperator {

    private List<TimeSeriesData> tupleList = new ArrayList<TimeSeriesData>();

    /**
     * Smoothing constant to use. The value should be between 1 and 0.
     *
     * If the value of alpha is higher, then the forecast tend to use demand as the basis for forecast.
     */
    @NotNull
    @Max (value = 1)
    @Min (value = 0)
    private double alpha;

    public void setAlpha(double alpha) {
        this.alpha = alpha;
    }

    public transient DefaultOutputPort<Double> forecastingPort = new DefaultOutputPort<Double>();

    public transient DefaultInputPort<TimeSeriesData> timeSeriesDataPort = new DefaultInputPort<TimeSeriesData>() {
        @Override
        public void process(TimeSeriesData tuple) {
            tupleList.add(tuple);
        }
    };

    /**
     *
     */
    @Override
    public void endWindow() {
        SEASmoothingForecaster seaSmoothingForecaster = new SEASmoothingForecaster();
        seaSmoothingForecaster.setAlpha(this.alpha);
        seaSmoothingForecaster.setTimeSeriesDataList(tupleList);
        forecastingPort.emit(seaSmoothingForecaster.computeForecast(true));
//        forecastingPort.emit(seaSmoothingForecaster.computeForecast(false));
        tupleList.clear();
    }

}
