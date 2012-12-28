/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.engine.TestCountSink;
import com.malhartech.lib.util.KeyValPair;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional test for {@link com.malhartech.lib.testbench.KeyValPairToHashMap}<p>
 * <br>
 */
public class KeyPairToHashMapTest {

    private static Logger log = LoggerFactory.getLogger(KeyPairToHashMapTest.class);


    /**
     * Test oper pass through. The Object passed is not relevant
     */
    @Test
    public void testNodeProcessing() throws Exception
    {
      KeyValPairToHashMap oper = new KeyValPairToHashMap();
      TestCountSink mapSink = new TestCountSink();

      oper.map.setSink(mapSink);
      oper.setup(new com.malhartech.engine.OperatorContext("irrelevant", null, null));

      oper.beginWindow(0);
      KeyValPair<String,String> input = new KeyValPair<String,String>("a", "1");

      // Same input object can be used as the oper is just pass through
      int numtuples = 1000;
      for (int i = 0; i < numtuples; i++) {
        oper.keyval.process(input);
      }
      oper.endWindow();

      Assert.assertEquals("number emitted tuples", numtuples, mapSink.count);
    }
}