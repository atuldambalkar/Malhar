/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.dag.TestSink;
import com.malhartech.lib.testbench.*;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.testbench.EventGenerator}. <p>
 * <br>
 * Load is generated and the tuples are outputted to ensure that the numbers are roughly in line with the weights<br>
 * <br>
 * Benchmarks:<br>
 * String schema generates over 11 Million tuples/sec<br>
 * HashMap schema generates over 1.7 Million tuples/sec<br>
 * <br>
 * DRC checks are validated<br>
 *
 */
public class BottomNBenchmark
{
  private static Logger log = LoggerFactory.getLogger(BottomNBenchmark.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new BottomN<String, Integer>());
    testNodeProcessingSchema(new BottomN<String, Double>());
    testNodeProcessingSchema(new BottomN<String, Float>());
    testNodeProcessingSchema(new BottomN<String, Short>());
    testNodeProcessingSchema(new BottomN<String, Long>());
  }

  public void testNodeProcessingSchema(BottomN oper)
  {
    TestSink<HashMap<String, Number>> sortSink = new TestSink<HashMap<String, Number>>();
    oper.bottom.setSink(sortSink);
    oper.setup(new OperatorConfiguration());
    oper.setN(3);

    oper.beginWindow();
    HashMap<String, Number> input = new HashMap<String, Number>();

    int numTuples = 5000000;
    for (int i = 0; i < numTuples; i++) {
      input.put("a", i);
      input.put("b", numTuples - i);
      oper.data.process(input);
    }
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 2, sortSink.collectedTuples.size());
    log.debug(String.format("\nBenchmaked %d tuples", numTuples));
  }
}