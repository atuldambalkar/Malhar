/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.machinelearning.timeseries;

import com.datatorrent.lib.machinelearning.timeseries.linearregression.SLRTimeSeries;
import com.datatorrent.lib.testbench.CollectorTestSink;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test class for SLRTimeSeries Forecaster operator.
 */
public class SLRTimeSeriesForecasterOperatorTest {

    @Test
    public void test() {
        SLRTimeSeriesForecasterOperator oper = new SLRTimeSeriesForecasterOperator();
        oper.setNumberOfTimeIntervalsInCycle(5);
        CollectorTestSink sink = new CollectorTestSink();

        oper.forecastOutputPort.setSink(sink);

        double[] stIt = { 0.89, 0.80, 1.05, 1.11, 1.16 };
        List<Double> stItList = new ArrayList<Double>();
        for (double value: stIt) {
            stItList.add(value);
        }

        SLRTimeSeries.Model model = new SLRTimeSeries.Model();
        model.intercept = 5.418;
        model.slope = 0.119;

        oper.stItPort.process(stItList);
        oper.slrTimeSeriesModelPort.process(model);

        oper.forecastInputPort.process(21);
        oper.forecastInputPort.process(22);
        oper.forecastInputPort.process(23);
        oper.forecastInputPort.process(24);
        oper.forecastInputPort.process(25);
        oper.forecastInputPort.process(56);
        oper.forecastInputPort.process(75);
        oper.forecastInputPort.process(80);
        oper.forecastInputPort.process(85);
        oper.forecastInputPort.process(5);
        oper.forecastInputPort.process(10);
        oper.forecastInputPort.process(12);
        oper.forecastInputPort.process(15);
        oper.forecastInputPort.process(18);
        oper.forecastInputPort.process(20);

        oper.endWindow();

        List tuples = sink.collectedTuples;

        Assert.assertEquals(7.05, (Double)tuples.get(0), 0.01);
        Assert.assertEquals(6.43, (Double)tuples.get(1), 0.01);
        Assert.assertEquals(8.57, (Double)tuples.get(2), 0.01);
        Assert.assertEquals(9.19, (Double)tuples.get(3), 0.01);
        Assert.assertEquals(9.74, (Double)tuples.get(4), 0.01);
    }
}
