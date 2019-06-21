package com.kafka.util;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Driver {

	@SuppressWarnings("resource")
	public static void main(final String[] args) throws Exception {
		final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
		final List<String> inputValues = Arrays.asList("dilip");

		final String INPUT_TOPIC = "streams-plaintext-input";

		final Properties producerConfig = new Properties();
		producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
		producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
		producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		final KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig, new StringSerializer(),
				new StringSerializer());

		// Always begin with writing the first line to the topic so that the REST API
		// example calls in
		// our step-by-step instructions always work right from the start (otherwise
		// users may run into
		// HTTP 404 errors when querying the latest value for a key, for example, until
		// the right input
		// data was sent to the topic).
		producer.send(new ProducerRecord<>(INPUT_TOPIC, inputValues.get(0), inputValues.get(0)));

		// every 500 milliseconds produce one of the lines of text from inputValues to
		// the
		// TextLinesTopic
		final Random random = new Random();
		while (true) {
			final int i = random.nextInt(inputValues.size());
			producer.send(new ProducerRecord<>(INPUT_TOPIC, inputValues.get(i), inputValues.get(i)));
			Thread.sleep(500L);
		}
	}
}
