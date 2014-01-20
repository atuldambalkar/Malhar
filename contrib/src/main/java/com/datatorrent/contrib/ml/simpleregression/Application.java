package com.datatorrent.contrib.ml.simpleregression;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.ConsoleOutputOperator;
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
        dag.setOutputPortAttribute(input.queryOutputPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);
        dag.setAttribute(input, Context.OperatorContext.INITIAL_PARTITION_COUNT, 2);

        OLSRegressionModelOperator olsRegression = dag.addOperator("olsRegression", OLSRegressionModelOperator.class);
        dag.setInputPortAttribute(olsRegression.inputPort, Context.PortContext.PARTITION_PARALLEL, true);
        dag.setInputPortAttribute(olsRegression.inputPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);
        dag.setOutputPortAttribute(olsRegression.simpleRegressionOutputPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);
        dag.setAttribute(olsRegression, Context.OperatorContext.APPLICATION_WINDOW_COUNT, 5);

        OLSRegressionModelUpdaterOperator olsModelUpdater = dag.addOperator("olsModelUpdater", OLSRegressionModelUpdaterOperator.class);
        dag.setInputPortAttribute(olsModelUpdater.simpleRegressionInputPort, Context.PortContext.PARTITION_PARALLEL, true);
        dag.setInputPortAttribute(olsModelUpdater.queryPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);
        dag.setOutputPortAttribute(olsModelUpdater.outputPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);
        dag.setAttribute(olsModelUpdater, Context.OperatorContext.APPLICATION_WINDOW_COUNT, 5);

        ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());

        dag.addStream("ingen", input.trainingDataOutputPort, olsRegression.inputPort);
        dag.addStream("querygen", input.queryOutputPort, olsModelUpdater.queryPort);
        dag.addStream("olsmodels", olsRegression.simpleRegressionOutputPort, olsModelUpdater.simpleRegressionInputPort);
        dag.addStream("olsresult", olsModelUpdater.outputPort, console.input);

    }
}
