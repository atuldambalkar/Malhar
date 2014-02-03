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

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

import javax.validation.constraints.NotNull;

/**
 * Operator to enrich the time series data received from external entity. This operator will append additional Time series related
 * data such as time interval within a time series cycle and the number of time series cycles (so far).
 */
public class TimeSeriesDataEnricherOperator extends BaseOperator {

    /**
     * Number of time intervals in a time-series cycle.
     */
    @NotNull
    private int numberOfTimeIntervalsInCycle;

    private int timeIntervalCount = 1;

    private int cycleCount = 1;

    public int getNumberOfTimeIntervalsInCycle() {
        return numberOfTimeIntervalsInCycle;
    }

    public void setNumberOfTimeIntervalsInCycle(int numberOfTimeIntervalsInCycle) {
        this.numberOfTimeIntervalsInCycle = numberOfTimeIntervalsInCycle;
    }

    public transient DefaultOutputPort<TimeSeriesData> timeSeriesDataPort =
            new DefaultOutputPort<TimeSeriesData>();

    public transient DefaultInputPort<Double> dataPort = new DefaultInputPort<Double>() {
        @Override
        public void process(Double y) {
            TimeSeriesData data = new TimeSeriesData();
            data.y = y;
            data.currentTimeInterval = timeIntervalCount;
            data.currentTimeCycle = cycleCount;
            if (timeIntervalCount == numberOfTimeIntervalsInCycle) {
                timeIntervalCount = 1;
                cycleCount++;
            }
            timeSeriesDataPort.emit(data);
        }
    };
}