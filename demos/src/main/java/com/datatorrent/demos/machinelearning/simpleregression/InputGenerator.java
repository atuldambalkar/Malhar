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
package com.datatorrent.demos.machinelearning.simpleregression;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.machinelearning.simpleregression.TrainingData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * Generate the training data tuples with single feature/variable liner regression model.
 *
 * Also generate query data.
 */
public class InputGenerator implements InputOperator {

    private static final Logger logger = LoggerFactory.getLogger(InputGenerator.class);
    private int blastCount = 10000;
    private Random random = new Random();

    @OutputPortFieldAnnotation(name = "trainingDataOutput")
    public final transient DefaultOutputPort<TrainingData> trainingDataOutputPort = new DefaultOutputPort<TrainingData>();

    @OutputPortFieldAnnotation(name = "queryDataOutput")
    public final transient DefaultOutputPort<Double> queryOutputPort = new DefaultOutputPort<Double>();


    public void setBlastCount(int blastCount) {
        this.blastCount = blastCount;
    }

    @Override
    public void beginWindow(long windowId) {
    }

    @Override
    public void endWindow() {
    }

    @Override
    public void setup(Context.OperatorContext context) {
    }

    @Override
    public void teardown() {
    }

    private int nextRandomId(int min, int max) {
        int id;
        do {
            id = (int) Math.abs(Math.round(random.nextGaussian() * max / 2));
        }
        while (id >= max);

        if (id < min) {
            id += min;
        }
        return id;
    }

    @Override
    public void emitTuples() {

        for (int i = 0; i < blastCount; ++i) {
            int size = nextRandomId(500, 3000);

            double price = 1.3 + 1.1 * size;
            this.trainingDataOutputPort.emit(new TrainingData(size, price));

            if (i % 1000 == 0) {
                this.queryOutputPort.emit(100.0 + size);
            }
        }
    }

}