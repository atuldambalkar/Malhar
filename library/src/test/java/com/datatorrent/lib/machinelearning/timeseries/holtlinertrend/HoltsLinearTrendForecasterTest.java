package com.datatorrent.lib.machinelearning.timeseries.holtlinertrend;

import com.datatorrent.lib.machinelearning.ModelCreationException;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test class for Holt's Linear Trend Forecaster.
 *
 * This test case refers to - https://www.otexts.org/fpp/7/2
 */
public class HoltsLinearTrendForecasterTest {

    @Test
    public void test() throws ModelCreationException {
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
        for (double value: values) {
            data.add(value);
        }

        double alpha = 0.8;
        double beta = 0.2;
        HoltsLinearTrendForecaster forecaster = new HoltsLinearTrendForecaster(alpha, beta);
        forecaster.setTimeSeriesData(data);
        double value = forecaster.computeForecast(16, true);

        Assert.assertEquals(43.780111567363925, value);

        value = forecaster.computeForecast(17, true);
        Assert.assertEquals(45.63152209583454, value);

        value = forecaster.computeForecast(18, true);
        Assert.assertEquals(47.48293262430516, value);

        value = forecaster.computeForecast(19, true);
        Assert.assertEquals(49.33434315277577, value);

        value = forecaster.computeForecast(20, true);
        Assert.assertEquals(51.18575368124639, value);
    }
}
