package com.kafka.streams;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;

import com.kafka.util.RestProxyService;
import com.kafka.util.RestServices;
import com.kafka.util.StreamConfig;

/*
# List all running instances of this application
* http://localhost:7070/state/instances
*
* # List app instances that currently manage (parts of) state store "word-count"
* http://localhost:7070/state/instances/word-count
*
* # Get all key-value records from the "word-count" state store hosted on a the instance running
* # localhost:7070
* http://localhost:7070/state/keyvalues/word-count/all
*
* # Find the app instance that contains key "hello" (if it exists) for the state store "word-count"
* http://localhost:7070/state/instance/word-count/hello
*
* # Get the latest value for key "hello" in state store "word-count"
* http://localhost:7070/state/keyvalue/word-count/hello
* */
public class WordCountKTableStreams {

	static final String DEFAULT_HOST = "localhost";
	static final int DEFAULT_PORT = 7070;

	public static void main(String[] args) throws Exception {

		final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
		// create stream
		final KafkaStreams streams = createStreams(
				StreamConfig.streamConf.apply("WordCountKTableStreams", bootstrapServers));
		// start the stream
		streams.start();
		// Start the Restful proxy for servicing remote access to state stores
		final RestServices restService = RestProxyService.startRestProxy(streams, DEFAULT_HOST, DEFAULT_PORT);

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

	// Example: Wait until the store of type T is queryable. When it is, return
	// a
	// reference to the store.
	public static <T> T waitUntilStoreIsQueryable(final String storeName,
			final QueryableStoreType<T> queryableStoreType, final KafkaStreams streams) throws InterruptedException {
		while (true) {
			try {
				return streams.store(storeName, queryableStoreType);
			} catch (InvalidStateStoreException ignored) {
				// store not yet ready for querying
				Thread.sleep(100);
			}
		}
	}

	static KafkaStreams createStreams(final Properties streamsConfiguration) {

		// Construct a `KStream` from the input topic "streams-plaintext-input",
		// where
		// message values
		// represent lines of text (for the sake of this example, we ignore
		// whatever may
		// be stored
		// in the message keys).
		final StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> textLines = builder.stream("streams-plaintext-input",
				Consumed.with(Serdes.String(), Serdes.String()));

		// textLines.print(Printed.toSysOut());

		KTable<String, Long> wordCounts = textLines
				// Split each text line, by whitespace, into words. The text
				// lines are the
				// message
				// values, i.e. we can ignore whatever data is in the message
				// keys and thus
				// invoke
				// `flatMapValues` instead of the more generic `flatMap`.
				.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
				// We use `groupBy` to ensure the words are available as message
				// keys
				.groupBy((key, value) -> value)
				// Create a key-value store named "CountsKeyValueStore" for the
				// all-time word
				// counts
				.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("word-count"));

		// Convert the `KTable<String, Long>` into a `KStream<String, Long>` and
		// write
		// to the output topic.
		wordCounts.toStream().to("streams-pipe-output", Produced.with(Serdes.String(), Serdes.Long()));
		return new KafkaStreams(builder.build(), streamsConfiguration);

	}

}
