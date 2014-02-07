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
import com.datatorrent.lib.machinelearning.timeseries.smoothing.centeredmovingaverage.MemoryOptimizedCMASmoothingOperator;
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
//        dag.setOutputPortAttribute(input.queryOutputPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);

        MemoryOptimizedCMASmoothingOperator cmaSmoothener = new MemoryOptimizedCMASmoothingOperator();
        cmaSmoothener.setNumberOfTimeIntervalsInCycle(24);
        dag.addOperator("cmaSmoothing", cmaSmoothener);

        DeseasonalizerOperator deseasonalizer = new DeseasonalizerOperator();
        deseasonalizer.setNumberOfTimeIntervalsInCycle(24);
        dag.addOperator("deseasonalizer", deseasonalizer);

        SLRTimeSeriesDataAggregator slrTimeSeriesAggregator = new SLRTimeSeriesDataAggregator();
        slrTimeSeriesAggregator.setTimeIntervalsInCycle(24);
        dag.addOperator("slrTimeSeries", slrTimeSeriesAggregator);

        SLRTimeSeriesForecastingOperator slrTimeSeriesForecaster = new SLRTimeSeriesForecastingOperator();
        slrTimeSeriesForecaster.setNumberOfTimeIntervalsInCycle(24);
        dag.addOperator("slrTimeSeriesForecaster", slrTimeSeriesForecaster);

        ConsoleOutputOperator console = dag.addOperator("forecastingConsole", ConsoleOutputOperator.class);

        dag.addStream("timeSeriesData", input.timeSeriesDataOutputPort, cmaSmoothener.timeSeriesDataPort);
        dag.addStream("cmaSmoothenedData", cmaSmoothener.cmaOutputPort, deseasonalizer.cmaSmoothenedTimeSeriesPort);
        dag.addStream("deseasonalizedDaa", deseasonalizer.deseasonalizedTimeSeriesPort, slrTimeSeriesAggregator.inputDataPort);
        dag.addStream("slrTimeSeriesModel", slrTimeSeriesAggregator.modelOutputPort, slrTimeSeriesForecaster.slrTimeSeriesModelPort);
        dag.addStream("stItData", deseasonalizer.stItPort, slrTimeSeriesForecaster.stItInputPort);
        dag.addStream("queryData", input.queryOutputPort, slrTimeSeriesForecaster.forecastInputPort);
        dag.addStream("forecaster", slrTimeSeriesForecaster.forecastOutputPort, console.input);
    }
}
