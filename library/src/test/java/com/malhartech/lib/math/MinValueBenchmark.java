/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.Sink;
import com.malhartech.dag.Tuple;
import java.util.ArrayList;
import java.util.List;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MinValueBenchmark
{
  private static Logger log = LoggerFactory.getLogger(MinValueBenchmark.class);

  class TestSink implements Sink
  {
    Object tuple = null;
    int count = 0;

    @Override
    public void process(Object payload)
    {
      if (payload instanceof Tuple) {
      }
      else {
        tuple = payload;
        count++;
      }
    }
  }

  /**
   * Test oper logic emits correct results
   */
  @Test
  @Category(com.malhartech.PerformanceTestCategory.class)
  public void testNodeSchemaProcessing()
  {
    MinValue<Double> oper = new MinValue<Double>();
    TestSink minSink = new TestSink();
    oper.min.setSink(minSink);

    // Not needed, but still setup is being called as a matter of discipline
    oper.setup(new OperatorConfiguration());
    oper.beginWindow(); //

    int numTuples = 100000000;
    for (int i = 0; i < numTuples; i++) {
      Double a = new Double(2.0);
      Double b = new Double(20.0);
      Double c = new Double(1000.0);

      oper.data.process(a);
      oper.data.process(b);
      oper.data.process(c);

      a = 1.0;
      oper.data.process(a);
      a = 10.0;
      oper.data.process(a);
      b = 5.0;
      oper.data.process(b);

      b = 12.0;
      oper.data.process(b);
      c = 22.0;
      oper.data.process(c);
      c = 14.0;
      oper.data.process(c);

      a = 46.0;
      oper.data.process(a);
      b = 2.0;
      oper.data.process(b);
      a = 23.0;
      oper.data.process(a);
    }
    oper.endWindow(); //
    log.debug(String.format("\nBenchmark for %d tuples; expected 1.0, got %f from %d tuples", numTuples*12,
                            (Double) minSink.tuple, minSink.count));
  }
}