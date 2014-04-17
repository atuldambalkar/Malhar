package com.datatorrent.lib.machinelearning.timeseries.holtlinertrend;

import com.datatorrent.lib.machinelearning.ForecastingException;
import com.datatorrent.lib.machinelearning.ModelCreationException;
import com.datatorrent.lib.machinelearning.ModelUpdationException;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test class for Holt's Linear Trend Forecaster.
 *
 * This test case refers to - https://www.otexts.org/fpp/7/2
 */
public class HoltsLinearTrendIncrementalForecasterTest {

    @Test
    public void test() throws ModelCreationException, ModelUpdationException, ForecastingException {
        double values[] = {
                17.55,
                21.86,
                23.89,
                26.93,
                26.89,
                28.83,
                30.08,
                30.95,
                30.19,
                31.58,
                32.58,
                33.48,
                39.02,
                41.39,
                41.60
        };

        List<Double> data = new ArrayList<Double>();
        for (int i = 0; i < 10; i++) {
            data.add(values[i]);
        }

        double alpha = 0.8;
        double beta = 0.2;
        HoltsLinearTrendIncrementalForecaster forecaster = new HoltsLinearTrendIncrementalForecaster(alpha, beta);
        forecaster.setTimeSeriesData(data);
        forecaster.buildModel();

        for (int i = 10; i < values.length; i++) {
            forecaster.incrementModel(values[i]);
        }

        double value = forecaster.computeForecast(16);
        Assert.assertEquals(43.780111567363925, value);

        value = forecaster.computeForecast(17);
        Assert.assertEquals(45.63152209583454, value);

        value = forecaster.computeForecast(18);
        Assert.assertEquals(47.48293262430516, value);

        value = forecaster.computeForecast(19);
        Assert.assertEquals(49.33434315277577, value);

        value = forecaster.computeForecast(20);
        Assert.assertEquals(51.18575368124639, value);
    }
}
