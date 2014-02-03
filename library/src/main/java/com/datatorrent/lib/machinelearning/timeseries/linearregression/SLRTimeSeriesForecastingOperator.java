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

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.machinelearning.timeseries.linearregression.SLRTimeSeries;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Forecaster operator to forecast value for given time period.
 */
public class SLRTimeSeriesForecastingOperator extends BaseOperator {

    @NotNull
    private int numberOfTimeIntervalsInCycle;

    private List<Double> stItList;
    private SLRTimeSeries.Model slrTimeSeriesModel;

    public void setNumberOfTimeIntervalsInCycle(int numberOfTimeIntervalsInCycle) {
        this.numberOfTimeIntervalsInCycle = numberOfTimeIntervalsInCycle;
    }

    public transient DefaultOutputPort<Double> forecastOutputPort = new DefaultOutputPort<Double>();

    public transient DefaultInputPort<List<Double>> stItPort = new DefaultInputPort<List<Double>>() {
        @Override
        public void process(List<Double> tuple) {
            stItList = tuple;
        }
    };

    public transient DefaultInputPort<SLRTimeSeries.Model> slrTimeSeriesModelPort = new DefaultInputPort<SLRTimeSeries.Model>() {
        @Override
        public void process(SLRTimeSeries.Model model) {
            slrTimeSeriesModel = model;
        }
    };

    public transient DefaultInputPort<Integer> forecastInputPort = new DefaultInputPort<Integer>() {
        @Override
        public void process(Integer timeValue) {
            if (stItList == null || slrTimeSeriesModel == null) {
                throw new IllegalStateException("Model is not ready yet!");
            }
            int timeIntervalInCycle = timeValue % numberOfTimeIntervalsInCycle;
            if (timeIntervalInCycle == 0) {
                timeIntervalInCycle = numberOfTimeIntervalsInCycle;
            }
            // (intercept + slope * timeValue) * stItForTimePeriod
            forecastOutputPort.emit((slrTimeSeriesModel.intercept + slrTimeSeriesModel.slope * timeValue) * stItList.get(timeIntervalInCycle - 1));
        }
    };
}
