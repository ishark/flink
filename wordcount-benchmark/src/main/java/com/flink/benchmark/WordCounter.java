package com.flink.benchmark;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.util.ArrayList;

import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCounter implements SinkFunction<byte[]>, Checkpointed<WordCounterState> {

	private boolean firstTime;
	private int count;
	private long startmillis;
	private ArrayList<Long> millis;
	private transient ArrayList<Integer> counts;

	public WordCounter() {
		firstTime = true;
		counts = new ArrayList<Integer>();
		millis = new ArrayList<Long>();
	}

	@Override
	public void invoke(byte[] arg0) throws Exception {
		// TODO Auto-generated method stub
		if (firstTime) {
			startmillis = System.currentTimeMillis();
			firstTime = false;
		}
		count++;
		if (count % 1000000 == 0) {
			// Add millis for 1 million
			logger.info("count = {}", count);
			long time = System.currentTimeMillis() - startmillis;
			logger.info("time = {}", time);
			logger.info("Average Records Processed per second = {}", count/time);
			millis.add(System.currentTimeMillis() - startmillis);
		} else if (count == Integer.MAX_VALUE) {
			logger.info("count = {}", count);
			long time = System.currentTimeMillis() - startmillis;
			logger.info("time = {}", time);
			logger.info("=========One round of int MAX_VALUE values=======");
			logger.info("=========Average tuples {}=========", count / time);
			count = 0;
			startmillis = System.currentTimeMillis();
		}
		if (millis.size() == 10) {
			millis.clear();
		}
	}

	private static final Logger logger = LoggerFactory.getLogger(WordCounter.class);

	@Override
	public WordCounterState snapshotState(long checkpointId, long checkpointTimestamp) throws Exception { //
		return new WordCounterState(firstTime, startmillis, millis, count);
	}

	@Override
	public void restoreState(WordCounterState state) throws Exception {
		this.firstTime = state.firstTime;
		this.startmillis = state.startmillis;
		this.millis = state.millis;
		this.count = state.count;
	}

}