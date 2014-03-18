package com.datatorrent.lib.machinelearning.timeseries.holtlinertrend;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.lib.machinelearning.ModelNotReadyException;
import com.datatorrent.lib.machinelearning.timeseries.TimeSeriesData;
import com.datatorrent.lib.machinelearning.timeseries.simpleexponentialaverage.SEASmoothingForecaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;

/**
 * Operator to handle time-series forecasting based on Holt's Linear Trend Model.
 *
 * This operator forecasts the value for the next time period in the process API as soon as it gets the list of time series data.
 */
@OperatorAnnotation (partitionable = false)
public class HoltsLinearTrendForecastingOperator extends BaseOperator {

    private HoltsLinearTrendForecaster holtsLinearTrendForecaster = null;

    private static final Logger logger = LoggerFactory.getLogger(HoltsLinearTrendForecastingOperator.class);

    /**
     * Exponential smoothing constant for Value
     */
    @NotNull
    @Max (value = 1)
    @Min (value = 0)
    private Double alpha;

    /**
     * Exponential smoothing constant for Slope
     */
    @NotNull
    @Max (value = 1)
    @Min (value = 0)
    private Double beta;

    public void setAlpha(double alpha) {
        this.alpha = alpha;
    }

    public void setBeta(Double beta) {
        this.beta = beta;
    }

    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        this.holtsLinearTrendForecaster = new HoltsLinearTrendForecaster(alpha, beta);
    }

    public transient DefaultOutputPort<Double> forecastingPort = new DefaultOutputPort<Double>();

    public transient DefaultInputPort<Integer> futureInputPort = new DefaultInputPort<Integer>() {
        @Override
        public void process(Integer future) {
            try {
                forecastingPort.emit(holtsLinearTrendForecaster.computeForecast(future, true));
            } catch (ModelNotReadyException exc) {
                logger.error("Holt's Linear Trend Model is not ready yet!", exc);
            }
        }
    };

    /**
     * Process the given list of time series data objects and forecast the value for next time period.
     */
    public transient DefaultInputPort<List<Double>> timeSeriesDataPort = new DefaultInputPort<List<Double>>() {
        @Override
        public void process(List<Double> tupleList) {
            holtsLinearTrendForecaster.setTimeSeriesData(tupleList);
        }
    };
}
