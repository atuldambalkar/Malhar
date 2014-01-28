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

import com.datatorrent.api.*;
import org.apache.commons.math3.stat.regression.SimpleRegression;

/**
 * Operator to merge the OLS regression models as received from upstream and generate a
 * combined model.
 */
public class OLSRegressionModelUnifier implements Operator.Unifier<SimpleRegression> {

    private SimpleRegression result = new SimpleRegression();

    public transient DefaultOutputPort<SimpleRegression> modelOutputPort = new DefaultOutputPort<SimpleRegression>();

    @Override
    public void setup(Context.OperatorContext operatorContext) {

    }

    @Override
    public void teardown() {

    }

    @Override
    public void beginWindow(long l) {

    }

    @Override
    public void endWindow() {
        modelOutputPort.emit(result);
    }

    @Override
    public void process(SimpleRegression simpleRegression) {
        result.append(simpleRegression);
    }

}
