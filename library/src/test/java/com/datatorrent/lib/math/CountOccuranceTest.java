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
package com.datatorrent.lib.math;

import junit.framework.Assert;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.testbench.CountOccurance;

import org.junit.Test;

/**
 */
public class CountOccuranceTest
{
	@SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
	public void testProcess()
	{
		CountOccurance oper = new CountOccurance();
		oper.setup(null);
		CollectorTestSink sink = new CollectorTestSink();
    oper.outport.setSink(sink);

    oper.beginWindow(1);
    oper.inport.process("a");
    oper.inport.process("b");
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, sink.collectedTuples.size());
	}
}
