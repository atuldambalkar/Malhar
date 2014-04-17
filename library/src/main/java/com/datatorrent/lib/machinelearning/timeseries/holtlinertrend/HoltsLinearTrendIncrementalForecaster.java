package com.datatorrent.lib.machinelearning.timeseries.holtlinertrend;

import com.datatorrent.lib.machinelearning.ForecastingException;
import com.datatorrent.lib.machinelearning.ModelCreationException;
import com.datatorrent.lib.machinelearning.ModelUpdationException;

import java.util.List;

/**
 * Class that implements Holt's Linear Trend Model for time series base forecasting with incremental update.
 *
 * For any given input data, in order to calculate Level - lsubt and Trend - bsubt, only the values for previous record such as
 * lsubt-1 and bsubt-1 are needed. After building the model based on the initial training data, this class only keeps track of the
 * previous values for Level and Trend and uses those values to update the new Level and Trend values.
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
public class HoltsLinearTrendIncrementalForecaster {

    /**
     * Exponential smoothing constant for Value
     */
    private Double alpha;

    /**
     * Exponential smoothing constant for Slope
     */
    private Double beta;

    /**
     * Number of data records used to build the model so far.
     */
    private int dataSize;

    private List<Double> data;

    private Double[] lcache;
    private Double[] bcache;
    private boolean modelReady;

    private double prevLevelsubt;
    private double prevTrendsubt;

    /**
     * Constructor
     *
     * @param alpha
     * @param beta
     */
    public HoltsLinearTrendIncrementalForecaster(double alpha, double beta) {
        this.alpha = alpha;
        this.beta = beta;
    }

    public void setTimeSeriesData(List<Double> data) {
        this.data = data;
    }

    public void buildModel() throws ModelCreationException {
        if (alpha == null || beta == null || data == null) {
            throw new ModelCreationException("Smoothing constants alpha, beta and Time Series data can't be empty!");
        }
        int size = data.size();
        this.dataSize = size;
        lcache = new Double[size + 1];
        bcache = new Double[size + 1];
        buildModel(size);
        lcache = null;
        bcache = null;
        modelReady = true;
    }

    /**
     * Build the model for given data.
     *
     * @param t
     * @return
     */
    private void buildModel(int t) {
        this.prevLevelsubt = buildLevel(t);
        this.prevTrendsubt = buildTrend(t);
    }

    private double buildLevel(int t) {
        if (t == 0) {
            return 0;
        }
        if (lcache[t] == null) {
            lcache[t] = (alpha * data.get(t - 1)) + ((1 - alpha) * (buildLevel(t - 1) + buildTrend(t - 1)));
        }
        return lcache[t];
    }

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
     * Use the given data to incrementally update the model.
     * @param data
     */
    public void incrementModel(double data) throws ModelUpdationException {
        if (!modelReady) {
            throw new ModelUpdationException("Model seems to be empty, hence can not be updated!");
        }
        // first calculate the lsubt and bsubt
        double lsubt = (alpha * data) + (1 - alpha) * (prevLevelsubt + prevTrendsubt);
        double bsubt = (beta * (lsubt - prevLevelsubt)) + ((1 - beta) * prevTrendsubt);

        // now update the prev values for lsubt and bsubt
        this.prevLevelsubt = lsubt;
        this.prevTrendsubt = bsubt;
        this.dataSize++;
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
        int stepAhead = future - dataSize;
        return prevLevelsubt + (stepAhead * prevTrendsubt);
    }
}
