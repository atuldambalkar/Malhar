package com.datatorrent.demos.machinelearning.simpleregression;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.machinelearning.simpleregression.OLSRegressionModelOperator;
import com.datatorrent.lib.machinelearning.simpleregression.OLSRegressionModelUnifier;
import org.apache.hadoop.conf.Configuration;

/**
 * Application class for SimpleRegression Model with an ordinary least squares regression model with one independent variable.
 *
 * @since
 */
public class Application implements StreamingApplication {

    @Override
    public void populateDAG(DAG dag, Configuration entries) {

        dag.setAttribute(DAG.APPLICATION_NAME, "SimpleRegressionApplication");

        InputGenerator input = dag.addOperator("input", InputGenerator.class);
        input.setBlastCount(10000);
        dag.setOutputPortAttribute(input.queryOutputPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);
        dag.setAttribute(input, Context.OperatorContext.INITIAL_PARTITION_COUNT, 2);

        OLSRegressionModelOperator olsRegression = dag.addOperator("olsRegression", OLSRegressionModelOperator.class);
        dag.setInputPortAttribute(olsRegression.inputPort, Context.PortContext.PARTITION_PARALLEL, true);
        dag.setAttribute(olsRegression, Context.OperatorContext.PARTITION_TPS_MIN, 50000);
        dag.setAttribute(olsRegression, Context.OperatorContext.PARTITION_TPS_MAX, 100000);
        dag.setAttribute(olsRegression, Context.OperatorContext.INITIAL_PARTITION_COUNT, 1);
        dag.setInputPortAttribute(olsRegression.inputPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);
        dag.setOutputPortAttribute(olsRegression.modelPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);
        dag.setAttribute(olsRegression, Context.OperatorContext.APPLICATION_WINDOW_COUNT, 5);

        PredictionOperator predictionOperator = dag.addOperator("prediction", PredictionOperator.class);

        ConsoleOutputOperator console = new ConsoleOutputOperator();
        dag.addOperator("console", console);

        dag.addStream("ingen", input.trainingDataOutputPort, olsRegression.inputPort);
        dag.addStream("querygen", input.queryOutputPort, predictionOperator.queryPort);
        dag.addStream("olsmodel", olsRegression.modelPort, predictionOperator.modelPort);
        dag.addStream("olsresult", predictionOperator.predictionPort, console.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    }
}
