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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Skeleton for a Flink Job.
 * 
 * For a full example of a Flink Job, see the WordCountJob.java file in the same
 * package/directory or have a look at the website.
 * 
 * You can also generate a .jar file that you can submit on your Flink cluster.
 * Just type mvn clean package in the projects root directory. You will find the
 * jar in target/flink-quickstart-0.1-SNAPSHOT-Sample.jar
 * 
 */
public class Job {

	public static void main(String[] args) throws Exception {
		// set up the execution environment

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		RandomeWordGenerator gen = new RandomeWordGenerator();
		gen.setEmitSameTuple(true);

		env.addSource(gen).disableChaining().addSink(new WordCounter());
	
		/**
		 * Here, you can start creating your execution plan for Flink.
		 * 
		 * Start with getting some data from the environment, like
		 * env.readTextFile(textPath);
		 * 
		 * then, transform the resulting DataSet<String> using operations like
		 * .filter() .flatMap() .join() .coGroup() and many more. Have a look at
		 * the programming guide for the Java API:
		 * 
		 * http://flink.apache.org/docs/latest/programming_guide.html
		 * 
		 * and the examples
		 * 
		 * http://flink.apache.org/docs/latest/examples.html
		 * 
		 */

		// execute program
		env.execute("Flink Java API Skeleton");
	}

	public static class RandomeWordGenerator implements SourceFunction<byte[]> {

		private volatile boolean cancelled = false;

		@Override
		public void cancel() {
			// TODO Auto-generated method stub
			cancelled = true;
		}

		@Override
		public void run(org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<byte[]> ctx) throws Exception {
			// Emit DataStream of random bytes
			final int TUPLE_SIZE_COPY = tupleSize;
			final boolean EMIT_SAME_TUPLE_COPY = emitSameTuple;
			while (!cancelled) {
				/*
				 * if (firstTime) { if (EMIT_SAME_TUPLE_COPY) { for (int i =
				 * count--; i-- > 0;) { ctx.collect(sameTupleArray); } } else {
				 * for (int i = count--; i-- > 0;) { ctx.collect(new
				 * byte[TUPLE_SIZE_COPY]); } } firstTime = false; } else
				 */{
					//logger.info("emitting new tuple");
					if (EMIT_SAME_TUPLE_COPY) {
						ctx.collect(sameTupleArray);
					} else {
						ctx.collect(new byte[TUPLE_SIZE_COPY]);
					}
					count++;
				}
			}
		}

		private transient int count;
		private boolean firstTime;

		private boolean emitSameTuple = false;
		byte[] sameTupleArray;
		private int tupleSize = 64;

		/**
		 * Sets the size of the tuple to the specified size. The change to tuple
		 * size takes effect in next window.
		 * 
		 * @param size
		 *            the tupleSize to set
		 */
		public void setTupleSize(int size) {
			tupleSize = size;
			if (emitSameTuple) {
				sameTupleArray = new byte[tupleSize];
			}
		}

		/**
		 * @return the tupleSize
		 */
		public int getTupleSize() {
			return tupleSize;
		}

		/**
		 * Sets the property that decides if the operator emits same tuple or
		 * creates new tuple for every emit. The change takes effect in next
		 * window.
		 * 
		 * @param isSameTuple
		 *            the boolean value to set for 'emitSameTuple' property
		 */
		public void setEmitSameTuple(boolean isSameTuple) {
			emitSameTuple = isSameTuple;
			if (isSameTuple) {
				sameTupleArray = new byte[tupleSize];
			}
		}

		/**
		 * @return the emitSameTuple property
		 */
		public boolean getEmitSameTuple() {
			return emitSameTuple;
		}

		private static final Logger logger = LoggerFactory.getLogger(RandomeWordGenerator.class);
	}

	public static class WordCounter implements SinkFunction<byte[]> {

		public WordCounter() {
			firstTime = true;
			counts = new ArrayList<Integer>();
			millis = new ArrayList<Integer>();
		}

		@Override
		public void invoke(byte[] arg0) throws Exception {
			// TODO Auto-generated method stub
			if (firstTime) {
				startmillis = System.currentTimeMillis();
				firstTime = false;
			}			
			count++;
			if(count % 10000 == 0) {
				logger.info("count = {}", count);
				logger.info("time = {}", System.currentTimeMillis() - startmillis);
			}
		}

		private boolean firstTime;
		private transient ArrayList<Integer> counts;
		private transient int count;
		private long startmillis;
		private ArrayList<Integer> millis;
		private static final Logger logger = LoggerFactory.getLogger(WordCounter.class);
	}
}
