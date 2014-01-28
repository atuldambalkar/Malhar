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
package com.datatorrent.lib.machinelearning.simpleregression;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.ShipContainingJars;
import org.apache.commons.math3.stat.regression.SimpleRegression;


/**
 * Operator to build a Ordinary Least Squares (OLS) Regression Model.
 *
 * It uses using SimpleRegression implementation from Apache Commons Math API.
 */
@ShipContainingJars(classes = {SimpleRegression.class})
public class OLSRegressionModelOperator extends BaseOperator {

    private SimpleRegression simpleRegression = new SimpleRegression();

    public transient DefaultOutputPort<SimpleRegression> modelPort = new DefaultOutputPort<SimpleRegression>() {
        @Override
        public Unifier<SimpleRegression> getUnifier() {
            return new OLSRegressionModelUnifier();
        }
    };

    public transient DefaultInputPort<TrainingData> inputPort = new DefaultInputPort<TrainingData>() {
        @Override
        public void process(TrainingData trainingData) {
            simpleRegression.addData(trainingData.x, trainingData.y);
        }
    };

    @Override
    public void beginWindow(long windowId) {
        simpleRegression.clear();
    }

    @Override
    public void endWindow() {
        modelPort.emit(simpleRegression);
    }
}
