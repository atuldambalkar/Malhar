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

import java.util.List;

/**
 * Test case for SLRTimeSeries Data aggregator operator.
 */
public class SLRTimeSeriesDataAggregatorTest {

    @Test
    public void test() {
        SLRTimeSeriesDataAggregator oper = new SLRTimeSeriesDataAggregator();
        oper.setTimeIntervalsInCycle(5);
        oper.setup(null);

        CollectorTestSink sink = new CollectorTestSink();

        oper.modelOutputPort.setSink(sink);

        double[][] data = {
                {5.39, 1, 1},
                {5.13, 2, 1},
                {5.71, 3, 1},
                {5.86, 4, 1},
                {6.03, 5, 1},
                {6.52, 1, 2},
                {6.50, 2, 2},
                {6.48, 3, 2},
                {6.67, 4, 2},
                {6.72, 5, 2},
                {6.74, 1, 3},
                {7.0, 2, 3},
                {7.14, 3, 3},
                {7.03, 4, 3},
                {7.16, 5, 3},
                {7.08, 1, 4},
                {7.38, 2, 4},
                {7.62, 3, 4},
                {7.57, 4, 4},
                {7.67, 5, 4},
        };

        for (double[] elem: data) {
            TimeSeriesData tuple = new TimeSeriesData();
            tuple.y = elem[0];
            tuple.currentTimeInterval = (int)elem[1];
            tuple.currentTimeCycle = (int)elem[2];
            oper.inputDataPort.process(tuple);
        }
        oper.endWindow();

        List tuples = sink.collectedTuples;

        SLRTimeSeries.Model model = (SLRTimeSeries.Model)tuples.get(0);

        Assert.assertEquals(5.41, model.intercept, 0.5);
        Assert.assertEquals(0.119, model.slope, .05);

    }
}
