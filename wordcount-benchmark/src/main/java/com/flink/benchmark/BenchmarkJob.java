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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
public class BenchmarkJob {

	public static enum ExecutionMode {
		CHAINED, NOT_CHAINED, ISOLATED_RESOURCES
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			showUsage();
			return;
		}

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(30 * 1000);

		// Setup generator
		ExecutionMode mode = ExecutionMode.valueOf(args[0]);
		int tupleSize = Integer.parseInt(args[1]);
		boolean sameTuple = Boolean.parseBoolean(args[2]);
		RandomWordGenerator gen = new RandomWordGenerator();
		gen.setEmitSameTuple(sameTuple);
		gen.setTupleSize(tupleSize);

		addDagToEnv(env, gen, mode);

		// execute program
		env.execute("Flink Java API Skeleton");
	}

	private static void showUsage() {
		System.err.println("USAGE:\nBenchmarkJob <Execution Mode> <TupleSize> <SameTuple>");
		System.err.println("<Execution Mode>:\tCHAINED for chained execution(Similar to thread_local)");
		System.err.println("\t\t\tNOT_CHAINED for non chained execution(Similar to container_local)");
		System.err.println("\t\t\tISOLATED_RESOURCES for isolated resources(operators on different containers, similar to no_locality)\n");
		System.err.println("<TupleSize>:\t\t64, 128, 256 etc.\n");
		System.err.println("<SameTuple>:\t\ttrue: Same tuple emitted");
		System.err.println("\t\t\tfalse: new tuple emitted by word generator");
	}

	public static void addDagToEnv(final StreamExecutionEnvironment env, RandomWordGenerator gen, ExecutionMode mode) {
		switch (mode) {
			case CHAINED:
				env.addSource(gen).addSink(new WordCounter());
				break;
			case NOT_CHAINED:
				env.addSource(gen).startNewChain().addSink(new WordCounter());
				break;
			case ISOLATED_RESOURCES:
				env.addSource(gen).isolateResources().addSink(new WordCounter());
				break;
			default:
				showUsage();
		}
	}

}
