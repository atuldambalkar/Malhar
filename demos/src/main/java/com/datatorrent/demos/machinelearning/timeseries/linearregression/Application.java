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
package com.datatorrent.demos.machinelearning.timeseries.linearregression;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.machinelearning.timeseries.linearregression.SLRTimeSeriesDataAggregator;
import com.datatorrent.lib.machinelearning.timeseries.linearregression.SLRTimeSeriesForecastingOperator;
import com.datatorrent.lib.machinelearning.timeseries.smoothing.centeredmovingaverage.DeseasonalizerOperator;
import com.datatorrent.lib.machinelearning.timeseries.smoothing.centeredmovingaverage.CMASmoothingOperator;
import org.apache.hadoop.conf.Configuration;

/**
 * Application  for handling Telecom call rate time series based prediction.
 *
 * @since
 */
public class Application implements StreamingApplication {

    @Override
    public void populateDAG(DAG dag, Configuration entries) {

        dag.setAttribute(DAG.APPLICATION_NAME, "TelecomCallRateTimeSeriesApplication");

        InputGenerator input = dag.addOperator("input", InputGenerator.class);
        dag.setOutputPortAttribute(input.queryOutputPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);

        CMASmoothingOperator cmaSmoothener = new CMASmoothingOperator();
        cmaSmoothener.setNumberOfTimeIntervalsInCycle(24);
        dag.addOperator("cmaSmoothing", cmaSmoothener);
        dag.setOutputPortAttribute(cmaSmoothener.cmaOutputPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);
        dag.setInputPortAttribute(cmaSmoothener.timeSeriesDataPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);

        DeseasonalizerOperator deseasonalizer = new DeseasonalizerOperator();
        deseasonalizer.setNumberOfTimeIntervalsInCycle(24);
        dag.addOperator("deseasonalizer", deseasonalizer);
        dag.setOutputPortAttribute(deseasonalizer.deseasonalizedTimeSeriesPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);
        dag.setOutputPortAttribute(deseasonalizer.stItPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);
        dag.setInputPortAttribute(deseasonalizer.cmaSmoothenedTimeSeriesPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);

        SLRTimeSeriesDataAggregator slrTimeSeriesAggregator = new SLRTimeSeriesDataAggregator();
        slrTimeSeriesAggregator.setTimeIntervalsInCycle(24);
        dag.addOperator("slrTimeSeries", slrTimeSeriesAggregator);
        dag.setOutputPortAttribute(slrTimeSeriesAggregator.modelOutputPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);
        dag.setInputPortAttribute(slrTimeSeriesAggregator.inputDataPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);

        SLRTimeSeriesForecastingOperator slrTimeSeriesForecaster = new SLRTimeSeriesForecastingOperator();
        slrTimeSeriesForecaster.setNumberOfTimeIntervalsInCycle(24);
        slrTimeSeriesForecaster.setTimeUnitMessage("Hours in future");
        slrTimeSeriesForecaster.setValueMessage("Calls");
        dag.addOperator("slrTimeSeriesForecaster", slrTimeSeriesForecaster);
        dag.setOutputPortAttribute(slrTimeSeriesForecaster.forecastOutputPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);
        dag.setInputPortAttribute(slrTimeSeriesForecaster.forecastInputPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);
        dag.setInputPortAttribute(slrTimeSeriesForecaster.slrTimeSeriesModelPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);

//        ConsoleOutputOperator valueConsole = dag.addOperator("forecastingValueConsole", ConsoleOutputOperator.class);
        ConsoleOutputOperator messageConsole = dag.addOperator("forecastingMessageConsole", ConsoleOutputOperator.class);

        dag.addStream("timeSeriesData", input.timeSeriesDataOutputPort, cmaSmoothener.timeSeriesDataPort);
        dag.addStream("cmaSmoothenedData", cmaSmoothener.cmaOutputPort, deseasonalizer.cmaSmoothenedTimeSeriesPort);
        dag.addStream("deseasonalizedDaa", deseasonalizer.deseasonalizedTimeSeriesPort, slrTimeSeriesAggregator.inputDataPort);
        dag.addStream("stItData", deseasonalizer.stItPort, slrTimeSeriesForecaster.stItInputPort);
        dag.addStream("slrTimeSeriesModel", slrTimeSeriesAggregator.modelOutputPort, slrTimeSeriesForecaster.slrTimeSeriesModelPort);
        dag.addStream("queryData", input.queryOutputPort, slrTimeSeriesForecaster.forecastInputPort);
//        dag.addStream("valueForecaster", slrTimeSeriesForecaster.forecastOutputPort, valueConsole.input);
        dag.addStream("valueMessageForecaster", slrTimeSeriesForecaster.forecastMessagePort, messageConsole.input);
    }
}
