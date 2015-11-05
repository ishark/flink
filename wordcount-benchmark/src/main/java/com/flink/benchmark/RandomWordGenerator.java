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

import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomWordGenerator implements SourceFunction<byte[]>, Checkpointed<WordGeneratorState> {
	private boolean emitSameTuple = false;
	public byte[] sameTupleArray;
	private int tupleSize = 64;

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
			// logger.info("emitting new tuple");
			if (EMIT_SAME_TUPLE_COPY) {
				ctx.collect(sameTupleArray);
			} else {
				ctx.collect(new byte[TUPLE_SIZE_COPY]);
			}
		}
	}

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
	 * creates new tuple for every emit. The change takes effect in next window.
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

	private static final Logger logger = LoggerFactory.getLogger(RandomWordGenerator.class);

	@Override
	public WordGeneratorState snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		return new WordGeneratorState(emitSameTuple, sameTupleArray, tupleSize, cancelled);
	}

	@Override
	public void restoreState(WordGeneratorState state) throws Exception {
		this.emitSameTuple = state.emitSameTuple;
		this.sameTupleArray = state.sameTupleArray;
		this.tupleSize = state.tupleSize;
		this.cancelled = state.cancelled;
	}
}