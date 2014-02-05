package com.datatorrent.lib.machinelearning.timeseries.simpleexponentialaverage;

import com.datatorrent.lib.machinelearning.timeseries.TimeSeriesData;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test case class for Simple Exponential Smoothing Forecaster algorithm.
 */
public class SEASmoothingForecasterTest {

    @Test
    public void test() {
        SEASmoothingForecaster forecaster = new SEASmoothingForecaster();
        forecaster.setAlpha(0.4);

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
        forecaster.setTimeSeriesDataList(tupleList);

        Assert.assertEquals(29.1232, forecaster.computeForecast(true), 0.000094);

    }
}
