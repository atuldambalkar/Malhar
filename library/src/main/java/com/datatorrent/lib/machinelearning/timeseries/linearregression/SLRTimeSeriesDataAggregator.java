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
package com.datatorrent.lib.machinelearning.timeseries.linearregression;

import com.datatorrent.api.*;
import com.datatorrent.lib.machinelearning.timeseries.TimeSeriesData;
import com.datatorrent.lib.machinelearning.timeseries.linearregression.SLRTimeSeries;

import javax.validation.constraints.NotNull;


/**
 * Operator class to calculate the required summation data for the given time series training data.
 * The data object emitted by this operator can be used to calculate that can be used to calculate/unify the total slope
 *
 * The slope data calculated gets incrementally updated in this operator.
 */
public class SLRTimeSeriesDataAggregator extends BaseOperator {

    /**
     * The number of time intervals for the given Time Series Cycle.
     * For example, to represent number of quarters within a year,the value for this variable will be 4.
     */
    @NotNull
    private int timeIntervalsInCycle;

    /**
     * Algorithm class.
     */
    private SLRTimeSeries slrTimeSeries;

    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        this.slrTimeSeries = new SLRTimeSeries(timeIntervalsInCycle);
    }

    public void setTimeIntervalsInCycle(int timeIntervalsInCycle) {
        this.timeIntervalsInCycle = timeIntervalsInCycle;
    }

    public transient DefaultOutputPort<SLRTimeSeries.Model> modelOutputPort = new DefaultOutputPort<SLRTimeSeries.Model>();

//    public transient DefaultOutputPort<SLRTimeSeries> modelOutputPort = new DefaultOutputPort<SLRTimeSeries>() {
//        @Override
//        public Unifier<SLRTimeSeries> getUnifier() {
//            return new SLRTimeSeriesUnifier();
//        }
//    };

    public transient DefaultInputPort<TimeSeriesData> inputDataPort = new DefaultInputPort<TimeSeriesData>() {
        @Override
        public void process(TimeSeriesData trainingData) {
            slrTimeSeries.process(trainingData);
        }
    };

    @Override
    public void beginWindow(long windowId) {
        super.beginWindow(windowId);
        slrTimeSeries.clear();
    }

    /**
     * Emit the slope data object along with the required data values..
     */
    @Override
    public void endWindow() {
//        modelOutputPort.emit(slrTimeSeries);
        modelOutputPort.emit(slrTimeSeries.getModel());
    }

}
