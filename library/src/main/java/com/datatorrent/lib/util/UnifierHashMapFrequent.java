/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.util;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.Unifier;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * Combiner for an output port that emits object with Map<K,V> interface and has the processing done
 * with sticky key partition, i.e. each one key belongs only to one partition. The final output of the
 * combiner is a simple merge into a single object that implements Map
 *
 *
 */
public class UnifierHashMapFrequent<K> implements Unifier<HashMap<K, Integer>>
{
  HashMap<K, Integer> mergedTuple = new HashMap<K, Integer>();
  public final transient DefaultOutputPort<HashMap<K, Integer>> mergedport = new DefaultOutputPort<HashMap<K, Integer>>();

  Integer lval;
  boolean least = true;
  /**
   * combines the tuple into a single final tuple which is emitted in endWindow
   * @param tuple incoming tuple from a partition
   */
  @Override
  public void process(HashMap<K, Integer> tuple)
  {
    // Tuple is HashMap<K,Integer>(1)
    if (mergedTuple.isEmpty()) {
      mergedTuple.putAll(tuple);
      for (Map.Entry<K, Integer> e: tuple.entrySet()) {
        lval = e.getValue();
        break;
      }
    }
    else {
      for (Map.Entry<K, Integer> e: tuple.entrySet()) {
        if ((least && (e.getValue() < lval))
                || (!least && (e.getValue() > lval))) {
          mergedTuple.clear();
          mergedTuple.put(e.getKey(), e.getValue());
          break;
        }
      }
    }
  }

  /**
   * getter function for combiner doing least (true) or most (false) compare
   * @return least flag
   */
  public boolean getLeast()
  {
    return least;
  }

  /**
   * setter funtion for combiner doing least (true) or most (false) compare
   * @param b
   */
  public void setLeast(boolean b)
  {
    least = b;
  }

  /**
   * a no op
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
  }

  /**
   * emits mergedTuple on mergedport if it is not empty
   */
  @Override
  public void endWindow()
  {
    if (!mergedTuple.isEmpty()) {
      mergedport.emit(mergedTuple);
      mergedTuple = new HashMap<K, Integer>();
    }
  }

  /**
   * a no-op
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
  }

  /**
   * a noop
   */
  @Override
  public void teardown()
  {
  }
}
