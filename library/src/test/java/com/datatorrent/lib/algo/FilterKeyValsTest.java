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
package com.datatorrent.lib.algo;

import com.datatorrent.lib.algo.FilterKeyVals;
import com.datatorrent.lib.testbench.CollectorTestSink;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.algo.FilterKeyVals}<p>
 *
 */
public class FilterKeyValsTest
{
  @SuppressWarnings("unchecked")
  int getTotal(List<Object> list)
  {
    int ret = 0;
    for (Object map: list) {
      for (Map.Entry<String, Number> e: ((HashMap<String, Number>)map).entrySet()) {
        ret += e.getValue().intValue();
      }
    }
    return ret;
  }

  /**
   * Test node logic emits correct results
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testNodeProcessing() throws Exception
  {
    FilterKeyVals<String,Number> oper = new FilterKeyVals<String,Number>();

    CollectorTestSink sortSink = new CollectorTestSink();
    oper.filter.setSink(sortSink);
    HashMap<String,Number> filter = new HashMap<String,Number>();
    filter.put("b",2);
    oper.setKeyVals(filter);
    oper.clearKeys();

    filter.clear();
    filter.put("e", 200);
    filter.put("f", 2);
    filter.put("blah", 2);
    oper.setKeyVals(filter);
    filter.clear();
    filter.put("a", 2);
    oper.setKeyVals(filter);

    oper.beginWindow(0);
    HashMap<String, Number> input = new HashMap<String, Number>();
    input.put("a", 2);
    input.put("b", 5);
    input.put("c", 7);
    input.put("d", 42);
    input.put("e", 202);
    input.put("e", 200);
    input.put("f", 2);
    oper.data.process(input);
    Assert.assertEquals("number emitted tuples", 3, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 204, getTotal(sortSink.collectedTuples));
    sortSink.clear();

    input.clear();
    input.put("a", 5);
    oper.data.process(input);
    Assert.assertEquals("number emitted tuples", 0, sortSink.collectedTuples.size());
    sortSink.clear();

    input.clear();
    input.put("a", 2);
    input.put("b", 33);
    input.put("f", 2);
    oper.data.process(input);
    Assert.assertEquals("number emitted tuples", 2, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 4, getTotal(sortSink.collectedTuples));
    sortSink.clear();

    input.clear();
    input.put("b", 6);
    input.put("a", 2);
    input.put("j", 6);
    input.put("e", 2);
    input.put("dd", 6);
    input.put("blah", 2);
    input.put("another", 6);
    input.put("notmakingit", 2);
    oper.data.process(input);
    Assert.assertEquals("number emitted tuples", 2, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 4, getTotal(sortSink.collectedTuples));
    sortSink.clear();

    input.clear();
    input.put("c", 9);
    oper.setInverse(true);
    oper.data.process(input);
    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    Assert.assertEquals("Total filtered value is ", 9, getTotal(sortSink.collectedTuples));

    oper.endWindow();
  }
}
