package com.datatorrent.lib.machinelearning.timeseries.holtwintersseasonal;

import com.datatorrent.lib.machinelearning.ForecastingException;
import com.datatorrent.lib.machinelearning.ModelCreationException;
import com.datatorrent.lib.machinelearning.ModelUpdationException;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test class for Holt-Winters' Multiplicative Method based Trend Forecaster.
 *
 * This test case refers to - https://www.otexts.org/fpp/7/2
 */
public class HoltWintersSeasonalMultiplicativeIncrementalForecasterTest {

    @Test
    public void test() throws ModelCreationException, ModelUpdationException, ForecastingException {
        double values[] = {
                112, 118, 132, 129, 121, 135, 148, 148, 136, 119, 104, 118,
                115, 126, 141, 135, 125, 149, 170, 170, 158, 133, 114, 140,
                145, 150, 178, 163, 172, 178, 199, 199, 184, 162, 146, 166,
                171, 180, 193, 181, 183, 218, 230, 242, 209, 191, 172, 194,
                196, 196, 236, 235, 229, 243, 264, 272, 237, 211, 180, 201,
                204, 188, 235, 227, 234, 264, 302, 293, 259, 229, 203, 229,
                242, 233, 267, 269, 270, 315, 364, 347, 312, 274, 237, 278
        };

        double incValues[] = {
                284, 277, 317, 313, 318, 374, 413, 405, 355, 306, 271, 306
        };

        List<Double> data = new ArrayList<Double>();
        for (double value: values) {
            data.add(value);
        }

        double alpha = 0.808981826;
        double beta = 0.017749398;
        double gamma = 1.03245357;
        HoltWintersSeasonalMultiplicativeIncrementalForecaster forecaster = new HoltWintersSeasonalMultiplicativeIncrementalForecaster(12, alpha, beta, gamma);
        forecaster.setTimeSeriesData(data);
        forecaster.buildModel();

        for (double value: incValues) {
            forecaster.incrementModel(value);
        }

        double value = forecaster.computeForecast(97);
        Assert.assertEquals(310.6246300650848, value);

        value = forecaster.computeForecast(98);
        Assert.assertEquals(317.454808844319, value);

        value = forecaster.computeForecast(103);
        Assert.assertEquals(492.0680293251041, value);

        value = forecaster.computeForecast(97);
        Assert.assertEquals(310.6246300650848, value);

        value = forecaster.computeForecast(100);
        Assert.assertEquals(318.3634418143311, value);

        value = forecaster.computeForecast(99);
        Assert.assertEquals(350.44673117194776, value);

        value = forecaster.computeForecast(110);
        Assert.assertEquals(341.84784498513517, value);
    }
}
