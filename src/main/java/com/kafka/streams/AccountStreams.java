package com.kafka.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.state.KeyValueStore;

import com.kafka.util.Constants;
import com.kafka.util.RestProxyService;
import com.kafka.util.RestServices;
import com.kafka.util.StreamConfig;

/**
 * 
 * @author dkumar
 *
 */
public class AccountStreams {

	public static void main(String[] args) throws Exception {

		final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
		// create stream
		final KafkaStreams streams = createStreams(StreamConfig.streamConf.apply("AccountStreams", bootstrapServers));
		streams.start();
		// Start the Restful proxy for servicing remote access to state stores
		final RestServices restService = RestProxyService.startRestProxy(streams, Constants.DEFAULT_HOST,
				Constants.DEFAULT_PORT);
		// Add shutdown hook to respond to SIGTERM and gracefully close Kafka
		// Streams
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
				streams.close();
				restService.stop();
			} catch (final Exception e) {
				// ignored
			}
		}));

	}

	/**
	 * joint-account 1230000 0 account a1 5000 == 5000 a2 10000 == 10000
	 * 
	 * 
	 * account a1 100 == 5100 a3 500 == 500 joint-account 1230000 15100 jerry
	 * 1000
	 * 
	 * @param streamsConfiguration
	 * @return
	 */
	static KafkaStreams createStreams(final Properties streamsConfiguration) {
		final StreamsBuilder builder = new StreamsBuilder();

		// access all kstore
		// http://localhost:7070/state/instances

		// joint account service
		// http://localhost:7070/state/keyvalues/joint-account-tr-store/all
		KTable<String, Long> jointAccountService = builder
				.stream("joint-account-tr", Consumed.with(Serdes.String(), Serdes.String()))
				.mapValues(x -> x.split(" ")).map((k, v) -> KeyValue.pair(v[0], Long.parseLong(v[1])))
				.groupByKey(Grouped.with(Serdes.String(), Serdes.Long())).reduce((v1, v2) -> v2,
						Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("joint-account-tr-store")
								.withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));
		jointAccountService.toStream().print(Printed.toSysOut());

		// account service
		// http://localhost:7070/state/keyvalues/account-tr-store/all
		KTable<String, Long> accountService = builder
				.stream("account-tr", Consumed.with(Serdes.String(), Serdes.String())).mapValues(x -> x.split(" "))
				.map((k, v) -> KeyValue.pair(v[0], Long.parseLong(v[1])))
				.groupByKey(Grouped.with(Serdes.String(), Serdes.Long())).reduce((v1, v2) -> v1 + v2,
						Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("account-tr-store")
								.withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));

		accountService.toStream().print(Printed.toSysOut());

		// update joint account service details
		// http://localhost:7070/state/keyvalues/joint-account-tr-main-store/all
		KTable<String, Long> jointJoinAccountService = jointAccountService.join(accountService,
				(leftValue, rightValue) -> leftValue + rightValue,
				Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("joint-account-tr-main-store")
						.withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));

		jointJoinAccountService.toStream().print(Printed.toSysOut());

		return new KafkaStreams(builder.build(), streamsConfiguration);

	}

}
