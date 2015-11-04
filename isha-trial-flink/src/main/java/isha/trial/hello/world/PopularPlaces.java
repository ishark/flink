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
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.sink.FileSinkFunctionByMillis;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction.WatchType;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class PopularPlaces {

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("USAGE:\nStreamTollNotifier <filepath to stream data>");
			return;
		}
		String filePath = args[0];
		
		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> taxiRides = env.readFileStream(filePath, 5, WatchType.PROCESS_ONLY_APPENDED);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<TaxiRide> rides = taxiRides.map(new MapFunction<String, TaxiRide>() {

			@Override
			public TaxiRide map(String value) throws Exception {
				return TaxiRide.fromString(value);
			}
		});

		rides.assignTimestamps(new TimeExtractorForTaxi());
		WindowedStream<Tuple3<Integer, Boolean, TaxiRide>, Tuple, TimeWindow> windowStream = rides.map(new MapGridCellId()).keyBy(0, 1)
		.timeWindow(Time.of(10, TimeUnit.SECONDS), Time.of(5, TimeUnit.SECONDS));
		// DataStream<Tuple4<Float, Float, Boolean, Integer>> output =
		SingleOutputStreamOperator<Tuple3<Integer, Boolean, Integer>, ?> output = 
				windowStream.apply(new WindowFunction<Tuple3<Integer, Boolean, TaxiRide>, Tuple3<Integer, Boolean, Integer>, Tuple, TimeWindow>() {
					@Override
					public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<Integer, Boolean, TaxiRide>> values,
							Collector<Tuple3<Integer, Boolean, Integer>> out) throws Exception {
						Tuple3<Integer, Boolean, Integer> cellCount = new Tuple3<Integer, Boolean, Integer>();
						System.out.println("Received values = " + values);
						;
						cellCount.f2 = 0;
						for (Tuple3<Integer, Boolean, TaxiRide> value : values) {
							// grid id
							cellCount.f0 = value.f0;
							// arriving or departing
							cellCount.f1 = value.f1;
							// increase counter
							cellCount.f2++;
						}

						// check threshold
						// if(cellCount.f2 >= popThreshold) {
						// emit record
						System.out.println("Received cell count = " + cellCount);
						out.collect(cellCount);
					}
				});
		
		FileSinkFunctionByMillis sink = new FileSinkFunctionByMillis<Tuple3<Integer, Boolean, Integer>>
		(new CsvOutputFormat(new Path("/media/isha/output.txt")), 1);
		output.addSink(sink);
		

//		FileSinkFunctionByMillis intermediateSink = new FileSinkFunctionByMillis<Tuple3<Integer, Boolean, Integer>>
//		(new CsvOutputFormat(new Path("/media/isha/intermediate.txt")), 1);
//		
		
//		windowStream.apply (new WindowFunction<Tuple3<Integer,Boolean,TaxiRide>, 
//				Tuple3<Integer,Boolean,TaxiRide>, Tuple, TimeWindow>() {
//
//			@Override
//			public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<Integer, Boolean, TaxiRide>> values,
//					Collector<Tuple3<Integer, Boolean, TaxiRide>> out) throws Exception {
//				// TODO Auto-generated method stub
//				for(Tuple3 t : values) {
//					//output.add(t);
//					out.collect(t);
//				}
//			
//			}
//		}).addSink(intermediateSink);
		
//		{
//		    public void apply (Tuple tuple,
//		            Window window,
//		            Iterable<Tuple2<String, Integer>> values,
//		            Collector<Integer> out) throws Exception {
//		        int sum = 0;
//		        for (value t: values) {
//		            sum += t.f1;
//		        }
//		        out.collect (new Integer(sum));
//		    }
//		};
		output.print();
		env.execute("Popular places");
	}

	public static class MapGridCellId implements MapFunction<TaxiRide, Tuple3<Integer, Boolean, TaxiRide>> {

		@Override
		public Tuple3<Integer, Boolean, TaxiRide> map(TaxiRide value) throws Exception {
			if (value.isStart) {
				return new Tuple3<Integer, Boolean, TaxiRide>(GeoUtils.mapToGridCell(value.startLat, value.startLon), true, value);
			} else {
				return new Tuple3<Integer, Boolean, TaxiRide>(GeoUtils.mapToGridCell(value.endLat, value.endLon), false, value);
			}
		}
	}

	public static class TimeExtractorForTaxi extends AscendingTimestampExtractor<TaxiRide> {

		@Override
		public long extractAscendingTimestamp(TaxiRide element, long currentTimestamp) {
			return element.time.getMillis();
		}

	}

	/*
	 * public static class WindowTriggerFunction implements
	 * CloneableTriggerPolicy<Tuple3<Integer, Boolean, TaxiRide>> { private
	 * LinkedList<Tuple3<Integer, Boolean, TaxiRide>> buffer = new
	 * LinkedList<Tuple3<Integer, Boolean, TaxiRide>>();
	 * 
	 * @Override public boolean notifyTrigger(Tuple3<Integer, Boolean, TaxiRide>
	 * datapoint) { this.buffer.add(datapoint); DateTime startTime =
	 * buffer.getFirst().f2.time;
	 * System.out.println("Trigger Time difference = " +
	 * (datapoint.f2.time.getMillis() - startTime.getMillis())); if
	 * (datapoint.f2.time.getMillis() - startTime.getMillis() >= 30 * 1000) {
	 * System.out.println("Triggereing  = " + (datapoint.f2.time.getMillis() -
	 * startTime.getMillis()) + datapoint.f2); while
	 * (datapoint.f2.time.getMillis() - startTime.getMillis() >= 30 * 1000) {
	 * System.out.println("Removing  = " +
	 * (buffer.getFirst().f2.time.getMillis() - startTime.getMillis()) + " , " +
	 * buffer.getFirst().f2); buffer.remove(); startTime =
	 * buffer.getFirst().f2.time; } return true; } return false; }
	 * 
	 * public WindowTriggerFunction() {
	 * 
	 * }
	 * 
	 * public WindowTriggerFunction(WindowTriggerFunction function) {
	 * this.buffer = function.buffer; }
	 * 
	 * public WindowTriggerFunction clone() { return new
	 * WindowTriggerFunction(this); }
	 * 
	 * // @Override // public boolean notifyTrigger(Tuple3<Integer, Boolean,
	 * TaxiRide> // datapoint) { // // TODO Auto-generated method stub // if(new
	 * Interval(datapoint.f2.time, startTime) == new Interval(5* 60 * // 1000))
	 * { // return true; // } // return false; // } }
	 */

	/*
	 * public static class WindowEviction implements
	 * CloneableEvictionPolicy<Tuple3<Integer, Boolean, TaxiRide>> { DateTime
	 * startTime; private LinkedList<Tuple3<Integer, Boolean, TaxiRide>> buffer
	 * = new LinkedList<Tuple3<Integer, Boolean, TaxiRide>>();
	 * 
	 * @Override public int notifyEviction(Tuple3<Integer, Boolean, TaxiRide>
	 * datapoint, boolean triggered, int bufferSize) {
	 * this.buffer.add(datapoint); startTime = buffer.getFirst().f2.time; int
	 * count = 0; System.out.println("Eviction Time difference = " +
	 * (datapoint.f2.time.getMillis() - startTime.getMillis())); if
	 * (datapoint.f2.time.getMillis() - startTime.getMillis() >= 60 * 1000) {
	 * while (datapoint.f2.time.getMillis() - startTime.getMillis() >= 1 * 60 *
	 * 1000) { System.out.println("Evicting  = " +
	 * (datapoint.f2.time.getMillis() - startTime.getMillis()) +
	 * buffer.getFirst().f2); buffer.remove(); startTime =
	 * buffer.getFirst().f2.time; count++; } return count; } return 0; }
	 * 
	 * public WindowEviction(WindowEviction function) { this.buffer =
	 * function.buffer; }
	 * 
	 * public WindowEviction clone() { return new WindowEviction(this); }
	 * 
	 * public WindowEviction() {
	 * 
	 * }
	 * 
	 * };
	 */

	public static class CountRides implements FlatMapFunction<Tuple3<Integer, Boolean, TaxiRide>, Tuple3<Integer, Boolean, Integer>> {

		HashMap<Integer, Integer> startRidesMapWihCellId = new HashMap<Integer, Integer>();
		HashMap<Integer, Integer> endRidesMapWihCellId = new HashMap<Integer, Integer>();

		@Override
		public void flatMap(Tuple3<Integer, Boolean, TaxiRide> value, Collector<Tuple3<Integer, Boolean, Integer>> out) throws Exception {
			if (value.f1) {
				int count = 0;
				if (startRidesMapWihCellId.containsKey(value.f0)) {
					count = startRidesMapWihCellId.get(value.f0);
				}
				startRidesMapWihCellId.put(value.f0, ++count);
			} else {
				int count = 0;
				if (endRidesMapWihCellId.containsKey(value.f0)) {
					count = endRidesMapWihCellId.get(value.f0);
				}
				endRidesMapWihCellId.put(value.f0, ++count);
			}
		}

	}

	public static class GridToCoordinates implements MapFunction<Tuple3<Integer, Boolean, Integer>, Tuple4<Float, Float, Boolean, Integer>> {

		@Override
		public Tuple4<Float, Float, Boolean, Integer> map(Tuple3<Integer, Boolean, Integer> cellCount) throws Exception {

			return new Tuple4<Float, Float, Boolean, Integer>(GeoUtils.getGridCellCenterLon(cellCount.f0),
					GeoUtils.getGridCellCenterLat(cellCount.f0), cellCount.f1, cellCount.f2);
		}
	}
}


/*
 * .apply(new WindowFunction<TaxiRide, R, Tuple, TimeWindow>() {
 * 
 * @Override public void apply(Tuple2 key, TimeWindow window,
 * Iterable<TaxiRide> values, Collector<Tuple3<Integer, Boolean,
 * Integer>> out) throws Exception { // TODO Auto-generated method stub
 * { Tuple3<Integer, Boolean, Integer> cellCount = new Tuple3<Integer,
 * Boolean, Integer>(); System.out.println("Received values = " +
 * values); ; cellCount.f2 = 0; for (Tuple3<Integer, Boolean, TaxiRide>
 * value : values) { // grid id cellCount.f0 = value.f0; // arriving or
 * departing cellCount.f1 = value.f1; // increase counter
 * cellCount.f2++; }
 * 
 * // check threshold // if(cellCount.f2 >= popThreshold) { // emit
 * record System.out.println("Received cell count = " + cellCount);
 * out.collect(cellCount); // } } );
 */
// taxiRides.assignTimestamps(new AscendingTimestampExtractor<TaxiRide>{
// @Override
// public long extractAscendingTimestamp(TaxiRide element, long
// currentTimestamp) {
// return element.time.getMillis();
// }
// });

// DataStream<Tuple4<Float, Float, Boolean, Integer>>
// .map(new MapGridCellId()).keyBy(0,1)
// .window(Count.of(10)).every(Count.of(3))
// // .timeWindow()
// //(new WindowTriggerFunction(), new WindowEviction())
// // .window(new CountTriggerPolicy(10), new WindowEviction())
// .mapWindow(new WindowMapFunction<Tuple3<Integer, Boolean, TaxiRide>,
// Tuple3<Integer, Boolean, Integer>>() {
// @Override
// public void mapWindow(Iterable<Tuple3<Integer, Boolean, TaxiRide>>
// values, Collector<Tuple3<Integer, Boolean, Integer>> out)
// throws Exception {
// Tuple3<Integer, Boolean, Integer> cellCount = new Tuple3<Integer,
// Boolean, Integer>();
// System.out.println("Received values = " + values);
// ;
// cellCount.f2 = 0;
// for (Tuple3<Integer, Boolean, TaxiRide> value : values) {
// // grid id
// cellCount.f0 = value.f0;
// // arriving or departing
// cellCount.f1 = value.f1;
// // increase counter
// cellCount.f2++;
// }
//
// // check threshold
// // if(cellCount.f2 >= popThreshold) {
// // emit record
// System.out.println("Received cell count = " + cellCount);
// out.collect(cellCount);
// // }
// }
// }).flatten().map(new GridToCoordinates());

// ((DataStream<String>) output.print();