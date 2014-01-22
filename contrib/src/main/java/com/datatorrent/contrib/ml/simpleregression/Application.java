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
        input.setBlastCount(10000);
        dag.setOutputPortAttribute(input.queryOutputPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);
        dag.setAttribute(input, Context.OperatorContext.INITIAL_PARTITION_COUNT, 2);

        OLSRegressionModelOperator olsRegression = dag.addOperator("olsRegression", OLSRegressionModelOperator.class);
        dag.setInputPortAttribute(olsRegression.inputPort, Context.PortContext.PARTITION_PARALLEL, true);
        dag.setAttribute(olsRegression, Context.OperatorContext.PARTITION_TPS_MIN, 50000);
        dag.setAttribute(olsRegression, Context.OperatorContext.PARTITION_TPS_MAX, 100000);
        dag.setAttribute(olsRegression, Context.OperatorContext.INITIAL_PARTITION_COUNT, 1);
        dag.setInputPortAttribute(olsRegression.inputPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);
        dag.setOutputPortAttribute(olsRegression.simpleRegressionOutputPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);
        dag.setAttribute(olsRegression, Context.OperatorContext.APPLICATION_WINDOW_COUNT, 5);

//        OLSRegressionModelAggregatorOperator olsModelAggregator =
//                dag.addOperator("olsModelAggregator", OLSRegressionModelAggregatorOperator.class);
//        dag.setInputPortAttribute(olsModelAggregator.simpleRegressionInputPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);
//        dag.setOutputPortAttribute(olsModelAggregator.aggregatedModelOutputPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);
//        dag.setAttribute(olsModelAggregator, Context.OperatorContext.APPLICATION_WINDOW_COUNT, 5);

        OLSRegressionModelUpdaterOperator olsModelUpdater = dag.addOperator("olsModelUpdater", OLSRegressionModelUpdaterOperator.class);
        dag.setInputPortAttribute(olsModelUpdater.queryPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);
        dag.setOutputPortAttribute(olsModelUpdater.outputPort, Context.PortContext.QUEUE_CAPACITY, 32 * 1024);
        dag.setAttribute(olsModelUpdater, Context.OperatorContext.APPLICATION_WINDOW_COUNT, 5);

        ConsoleOutputOperator console = new ConsoleOutputOperator();
        dag.addOperator("console", console);

        dag.addStream("ingen", input.trainingDataOutputPort, olsRegression.inputPort);
        dag.addStream("querygen", input.queryOutputPort, olsModelUpdater.queryPort);
        dag.addStream("olsmodels", olsRegression.simpleRegressionOutputPort, olsModelUpdater.modelInputPort);
//        dag.addStream("olsmodels", olsRegression.simpleRegressionOutputPort, olsModelAggregator.simpleRegressionInputPort);
//        dag.addStream("olsaggrmodels", olsModelAggregator.aggregatedModelOutputPort, olsModelUpdater.modelInputPort);
        dag.addStream("olsresult", olsModelUpdater.outputPort, console.input);
    }
}
