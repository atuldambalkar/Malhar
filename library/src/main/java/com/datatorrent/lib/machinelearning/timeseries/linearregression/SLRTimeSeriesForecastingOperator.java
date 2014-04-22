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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Forecaster operator to forecast value for given time period.
 *
 * Forecast value for the given time value is calculated using following formula -
 *
 * Forecast value = (intercept + slope * timeValue) * stItForTimePeriod
 */
public class SLRTimeSeriesForecastingOperator extends BaseOperator {

    @NotNull
    private int numberOfTimeIntervalsInCycle;

    @NotNull
    private String timeUnitMessage;

    @NotNull
    private String valueMessage;

    private List<Double> stItList;
    private SLRTimeSeries.Model slrTimeSeriesModel;
    private long windowId;

    private static final Logger logger = LoggerFactory.getLogger(SLRTimeSeriesForecastingOperator.class);

    public void setNumberOfTimeIntervalsInCycle(int numberOfTimeIntervalsInCycle) {
        this.numberOfTimeIntervalsInCycle = numberOfTimeIntervalsInCycle;
    }

    public void setTimeUnitMessage(String timeUnitMessage) {
        this.timeUnitMessage = timeUnitMessage;
    }

    public void setValueMessage(String valueMessage) {
        this.valueMessage = valueMessage;
    }

    @Override
    public void beginWindow(long windowId) {
        this.windowId = windowId;
    }

    public transient DefaultOutputPort<Double> forecastOutputPort = new DefaultOutputPort<Double>();
    public transient DefaultOutputPort<String> forecastMessagePort = new DefaultOutputPort<String>();

    public transient DefaultInputPort<List<Double>> stItInputPort = new DefaultInputPort<List<Double>>() {
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
                logger.info("WindowId: " + windowId + ", Model is not ready yet!");
                return;
            }

            int timeIntervalInCycle = timeValue % numberOfTimeIntervalsInCycle;
            if (timeIntervalInCycle == 0) {
                timeIntervalInCycle = numberOfTimeIntervalsInCycle;
            }
            double forecast = (slrTimeSeriesModel.intercept + slrTimeSeriesModel.slope * timeValue) * stItList.get(timeIntervalInCycle - 1);
            logger.debug("WindowId: " + windowId + ", Query: " + timeValue + " Intercept:" + slrTimeSeriesModel.slope + " Slope:" + slrTimeSeriesModel.intercept + " Forecast: " + forecast);
            StringBuffer forecastBuffer = new StringBuffer().append("Future time period: ")
                    .append(timeValue).append(" ").append(timeUnitMessage)
                    .append(", Forecasted value: ").append(forecast).append(" ").append(valueMessage);
            // (intercept + slope * timeValue) * stItForTimePeriod
            forecastOutputPort.emit(forecast);
            forecastMessagePort.emit(forecastBuffer.toString());
        }
    };
}