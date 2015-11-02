package isha.trial.hello.world;

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

import java.util.HashMap;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple9;

/**
 * Skeleton for a Flink Job.
 *
 * For a full example of a Flink Job, see the WordCountJob.java file in the
 * same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/flink-quickstart-0.1-SNAPSHOT-Sample.jar
 *
 */
public class Job {

//	enum Type {
//		PositionReport,
//		NoReport,
//		AccountBalanceQuery,
//		DailyExpenditureQuery, 
//		TravelTimeQuery
//	}
//	
//	static class InputQuery {
//		public Type type;
//		public Integer time;
//		public Integer vID;
//		public Integer speed;
//		public Integer xWay;
//		public Integer lane; 
//		public Integer dir;
//		public Integer segment; 
//		public Integer position; 
//	}
	
	HashMap<Integer, Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> lastVidReport = new HashMap<Integer, Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>();
	
	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input = env.readCsvFile("/media/isha/linear/data/10m1x.dat")
				.fieldDelimiter(",")
				.types(Integer.class, Integer.class, Integer.class, Integer.class, Integer.class, Integer.class, Integer.class, Integer.class, Integer.class);

		
		DataSet<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> reports = 
				input.filter(new Type0QueryFilter());
		
		//reports.flatMap(new);
		/**
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataSet<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/programming_guide.html
		 *
		 * and the examples
		 *
		 * http://flink.apache.org/docs/latest/examples.html
		 *
		 */
		reports.print();
		// execute program
		//env.execute("Flink Java API Skeleton");
	}
	
//	public static final class storeLastVidReport implements 
//	FlatMapFunction<Tuple9<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>, R>() {
//		
//	}
	
	public static final class Type0QueryFilter implements 
	FilterFunction<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> {
		@Override
		public boolean filter(
				Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> value)
				throws Exception {
			
			return value.f0 == 0;
		}
	}
}
