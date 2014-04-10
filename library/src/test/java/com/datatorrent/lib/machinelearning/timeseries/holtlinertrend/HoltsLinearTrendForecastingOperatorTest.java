package com.datatorrent.lib.machinelearning.timeseries.holtlinertrend;

import com.datatorrent.lib.machinelearning.ModelCreationException;
import com.datatorrent.lib.testbench.CollectorTestSink;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test class for Holt's Linear Trend Forecaster.
 *
 * This test case refers to - https://www.otexts.org/fpp/7/2
 */
public class HoltsLinearTrendForecastingOperatorTest {

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
        HoltsLinearTrendForecastingOperator operator = new HoltsLinearTrendForecastingOperator();
        CollectorTestSink sink = new CollectorTestSink();
        operator.setAlpha(alpha);
        operator.setBeta(beta);
        operator.setup(null);
        operator.timeSeriesDataPort.process(data);
        operator.forecastingPort.setSink(sink);

        operator.futureInputPort.process(16);

        Assert.assertEquals(45.721806161931184, sink.collectedTuples.get(0));

        operator.futureInputPort.process(17);
        Assert.assertEquals(47.61177184568906, sink.collectedTuples.get(1));

        operator.futureInputPort.process(18);
        Assert.assertEquals(49.50173752944694, sink.collectedTuples.get(2));

        operator.futureInputPort.process(19);
        Assert.assertEquals(51.39170321320482, sink.collectedTuples.get(3));
    }
}
