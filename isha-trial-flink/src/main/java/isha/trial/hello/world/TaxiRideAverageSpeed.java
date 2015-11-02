package isha.trial.hello.world;

import java.util.HashMap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction.WatchType;
import org.apache.flink.util.Collector;


public class TaxiRideAverageSpeed {

	
	
	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("USAGE:\nStreamTollNotifier <filepath to stream data>");
			return;
		}
		String filePath = args[0];

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> taxiRides = env.readFileStream(filePath, 5, WatchType.PROCESS_ONLY_APPENDED);
		DataStream<Tuple2<Long, Float>> rides = taxiRides.map(new MapFunction<String, TaxiRide>() {

			@Override
			public TaxiRide map(String value) throws Exception {				
				return TaxiRide.fromString(value);
			}
		})
		.keyBy("rideId")
		.flatMap(new RideEventJoiner()).map(new SpeedComputer());
		
		rides.print();
		env.execute();
	
		
	}
	

	/**
	 * Matches start and end TaxiRide records.
	 */
	public static class RideEventJoiner implements FlatMapFunction<TaxiRide, Tuple2<TaxiRide, TaxiRide>> {

		private HashMap<Long, TaxiRide> startRecords = new HashMap<Long, TaxiRide>();
		private Tuple2<TaxiRide, TaxiRide> joinedEvents = new Tuple2<TaxiRide, TaxiRide>();

		@Override
		public void flatMap(TaxiRide rideEvent, Collector<Tuple2<TaxiRide, TaxiRide>> out) throws Exception {

			if(rideEvent.isStart) {
				// remember start record
				startRecords.put(rideEvent.rideId, rideEvent);
			}
			else {
				// get and forget start record
				TaxiRide startRecord = startRecords.remove(rideEvent.rideId);
				if(startRecord != null) {
					// return start and end record
					joinedEvents.f0 = startRecord;
					joinedEvents.f1 = rideEvent;
					out.collect(joinedEvents);
				}
			}
		}
	}

	/**
	 * Computes the average speed of a taxi ride from its start and end record.
	 */
	public static class SpeedComputer implements MapFunction<Tuple2<TaxiRide, TaxiRide>, Tuple2<Long, Float>> {

		private static int MILLIS_PER_HOUR = 1000 * 60 * 60;
		private Tuple2<Long, Float> outT = new Tuple2<Long, Float>();

		@Override
		public Tuple2<Long, Float> map(Tuple2<TaxiRide, TaxiRide> joinedEvents) throws Exception {

			float distance = joinedEvents.f1.travelDistance;
			long startTime = joinedEvents.f0.time.getMillis();
			long endTime = joinedEvents.f1.time.getMillis();

			float speed;
			long timeDiff = endTime - startTime;
			if(timeDiff != 0) {
				// speed = distance / time
				speed = (distance / timeDiff) * MILLIS_PER_HOUR;
			}
			else {
				speed = -1;
			}

			// set ride Id
			outT.f0 = joinedEvents.f0.rideId;
			// compute speed
			outT.f1 = speed;

			return outT;
		}
	}
	
	public static class SpeedComputerFlatMap implements FlatMapFunction<TaxiRide, Tuple2<Long, Float>> {

		@Override
		public void flatMap(TaxiRide value, Collector<Tuple2<Long, Float>> out) throws Exception {
			
		}
		
	}
}
