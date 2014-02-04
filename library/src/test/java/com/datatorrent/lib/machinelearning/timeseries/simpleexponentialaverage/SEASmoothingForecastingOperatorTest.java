package com.datatorrent.lib.machinelearning.timeseries.simpleexponentialaverage;

import com.datatorrent.lib.machinelearning.timeseries.TimeSeriesData;
import com.datatorrent.lib.testbench.CollectorTestSink;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test case class for Simple Exponential Smoothing Forecasting Operator.
 */
public class SEASmoothingForecastingOperatorTest {

    @Test
    public void test() {
        SEASmoothingForecastingOperator oper = new SEASmoothingForecastingOperator();
        oper.setAlpha(0.4);
        oper.setup(null);

        CollectorTestSink sink = new CollectorTestSink();
        oper.forecastingPort.setSink(sink);

        double[][] data = {
                {28, 1, 1},
                {32, 2, 1},
                {30, 3, 1},
                {24, 4, 1},
                {26, 1, 2},
                {35, 2, 2},
                {30, 3, 2},
                {25, 4, 2},
                {27, 1, 3},
                {26, 2, 3},
                {28, 3, 3},
                {30, 4, 3},
                {31, 1, 4},
                {32, 2, 4},
                {29, 3, 4},
                {28, 4, 4},
        };
        List<TimeSeriesData> tupleList = new ArrayList<TimeSeriesData>();

        for (double[] elem: data) {
            TimeSeriesData tuple = new TimeSeriesData();
            tuple.y = elem[0];
            tuple.currentTimeInterval = (int)elem[1];
            tuple.currentTimeCycle = (int)elem[2];
            tupleList.add(tuple);
        }
        oper.timeSeriesDataPort.process(tupleList);

        List tuples = sink.collectedTuples;
        Assert.assertEquals(29.1232, (Double)tuples.get(0), 0.000094);

    }
}
