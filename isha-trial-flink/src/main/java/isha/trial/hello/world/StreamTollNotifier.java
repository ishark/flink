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
package isha.trial.hello.world;

import java.util.HashMap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamTollNotifier {
	public enum Type {
		PositionReport, NoReport, AccountBalanceQuery, DailyExpenditureQuery, TravelTimeQuery
	}

	public static class InputReport {
		public Type type;
		public Integer time;
		public Integer vID;
		public Integer speed;
		public Integer xWay;
		public Integer lane;
		public Integer dir;
		public Integer segment;
		public Integer position;
		public long entryTime;

		public InputReport() {

		}

		public InputReport(String[] tokens) {
			int index = 0;
			this.type = Integer.parseInt(tokens[index++]) == 0 ? Type.PositionReport : Type.AccountBalanceQuery;
			this.time = Integer.parseInt(tokens[index++]);
			this.vID = Integer.parseInt(tokens[index++]);
			this.speed = Integer.parseInt(tokens[index++]);
			this.xWay = Integer.parseInt(tokens[index++]);
			this.lane = Integer.parseInt(tokens[index++]);
			this.dir = Integer.parseInt(tokens[index++]);
			this.segment = Integer.parseInt(tokens[index++]);
			this.position = Integer.parseInt(tokens[index++]);
			this.entryTime = System.currentTimeMillis();
		}

		public boolean isExitLane() {

			if (this.lane == 4) {
				return true;
			}
			return false;
		}
		@Override
		public String toString() {
			// return String.format("type = {}, vid = {}, entry time = {}",
			// type, vID, this.entryTime);
			return "PositionReport{" + "time=" + time + ", vehicleId=" + vID + ", vehicleSpeed=" + speed + ", expressWayId=" + xWay + ", lane="
					+ lane + ", direction=" + dir + ", segment=" + segment + ", position=" + position + ", entry time=" + entryTime + '}';
		}
	}

	public static class TollNotifierOutput {
		public Integer type;
		public Integer vID;
		public Integer time;
		public Long emit;
		public Long latestAverageSpeed;
		public Long toll;

		public TollNotifierOutput() {
			
		}
		public TollNotifierOutput(Integer type, Integer vID, Integer time, Long emit, Long avgSpeed, Long toll) {
			this.type = type;
			this.vID = vID;
			this.time = time;
			this.emit = emit;
			this.latestAverageSpeed = avgSpeed;
			this.toll = toll;
		}

		@Override
		public String toString() {
			return "PositionReport{" + "type=" + type + ", vehicleId=" + vID + ", time=" + time + ", emit=" + emit + ", latestAverageSpeed="
					+ latestAverageSpeed + ", toll=" + toll + '}';
		}

	}

	public static void main(String[] args) throws Exception {

		if (args.length != 1) {
			System.err.println("USAGE:\nStreamTollNotifier <filepath to stream data>");
			return;
		}
		String filePath = args[0];

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataStream<String> positionReport = env.readTextFile(filePath);
		
		
		
		DataStream<TollNotifierOutput> reports = positionReport.map(new LineSplitter()).flatMap(new TollNotifyTrigger());
		DataStream<Tuple6<Integer, Integer, Integer, Long, Long, Long>> tupleReports = reports.map(new ConvertToTuple());
		tupleReports.writeAsCsv("/media/isha/linear/data/tempOut.csv");
		
		// .filter(new FilterFunction<StreamTollNotifier.InputReport>() {
		// @Override
		// public boolean filter(InputReport value) throws Exception {
		// return value.type == Type.PositionReport;
		// }
		// });

		// execute program
		env.execute("Toll Notification Example");
		
		
	}

	public static final class ConvertToTuple implements MapFunction<TollNotifierOutput, Tuple6<Integer, Integer, Integer, Long, Long, Long>> {

		@Override
		public Tuple6<Integer, Integer, Integer, Long, Long, Long> map(TollNotifierOutput value) throws Exception {
			return new Tuple6<Integer, Integer, Integer, Long, Long, Long> (value.type, value.vID, value.time, value.emit, value.latestAverageSpeed, value.toll);
		}
		
	}
	public static final class TollNotifyTrigger implements FlatMapFunction<InputReport, TollNotifierOutput> {

		HashMap<Integer, InputReport> latestReport = new HashMap<Integer, InputReport>();

		@Override
		public void flatMap(InputReport value, Collector<TollNotifierOutput> out) throws Exception {
			if (value.type == Type.PositionReport) {
				InputReport lastReport = latestReport.get(value.vID);
				if (lastReport != null) {
					if (lastReport.segment != value.segment && lastReport.dir  == value.dir && !value.isExitLane()) {
						// Trigger toll
						System.out.println("input value = " + value);
						long emitTime = value.time + (System.currentTimeMillis() - value.entryTime) / 1000;
						out.collect(new TollNotifierOutput(0, value.vID, value.time, (long) emitTime, (long) 0, (long) 0));
					}
				} else {
					// First position report for vehicle, toll is calculated
					if(!value.isExitLane()) {
						System.out.println("first report input value = " + value);
						long emitTime = value.time + (System.currentTimeMillis() - value.entryTime) / 1000;
						out.collect(new TollNotifierOutput(0, value.vID, value.time, (long) emitTime, (long) 0, (long) 0));
					}
				}
				latestReport.put(value.vID, value);
			}
		}
	}

	public static final class LineSplitter implements MapFunction<String, InputReport> {

		@Override
		public InputReport map(String value) throws Exception {
			String[] tokens = value.toLowerCase().split("\\W+");
			InputReport report = new InputReport(tokens);
			return report;
		}
	}
}
