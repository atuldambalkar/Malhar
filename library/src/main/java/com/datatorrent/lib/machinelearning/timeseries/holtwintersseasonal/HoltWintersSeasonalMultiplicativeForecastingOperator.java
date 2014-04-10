package com.datatorrent.lib.machinelearning.timeseries.holtwintersseasonal;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.lib.machinelearning.ModelCreationException;
import com.datatorrent.lib.machinelearning.timeseries.holtlinertrend.HoltsLinearTrendForecaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.nio.channels.NotYetBoundException;
import java.util.List;

/**
 * Operator to handle time-series forecasting based on Holt Winter's Seasonal Multiplicative Forecasting Model.
 *
 * This operator forecasts the value for the next time period in the process API as soon as it gets the list of time series data.
 */
@OperatorAnnotation (partitionable = false)
public class HoltWintersSeasonalMultiplicativeForecastingOperator extends BaseOperator {

    private HoltWintersSeasonalMultiplicativeForecaster holtWintersSeasonalMultiplicativeForecaster = null;

    private static final Logger logger = LoggerFactory.getLogger(HoltWintersSeasonalMultiplicativeForecastingOperator.class);

    /**
     * Number of periods in time series cycle. There has to be at-least 2 periods in a time-series cycle.
     */
    @NotNull
    @Min(value = 2)
    private int numPeriods;

    /**
     * Exponential smoothing constant for Level
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

    /**
     * Exponential smoothing constant for Seasonality
     */
    @NotNull
    private Double gamma;

    public void setNumPeriods(int numPeriods) {
        this.numPeriods = numPeriods;
    }

    public void setAlpha(double alpha) {
        this.alpha = alpha;
    }

    public void setBeta(Double beta) {
        this.beta = beta;
    }

    public void setGamma(Double gamma) {
        this.gamma = gamma;
    }

    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        try {
            this.holtWintersSeasonalMultiplicativeForecaster = new HoltWintersSeasonalMultiplicativeForecaster(numPeriods, alpha, beta, gamma);
        } catch (ModelCreationException exc) {
            logger.error("Exception during mode creation for Holt-Winter's Forecasting operator", exc);
        }
    }

    public transient DefaultOutputPort<Double> forecastingPort = new DefaultOutputPort<Double>();

    public transient DefaultInputPort<Integer> futureInputPort = new DefaultInputPort<Integer>() {
        @Override
        public void process(Integer future) {
            try {
                forecastingPort.emit(holtWintersSeasonalMultiplicativeForecaster.computeForecast(future, true));
            } catch (ModelCreationException exc) {
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
            holtWintersSeasonalMultiplicativeForecaster.setTimeSeriesData(tupleList);
        }
    };
}
