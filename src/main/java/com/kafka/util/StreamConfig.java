package com.kafka.util;

import java.util.Properties;
import java.util.function.BiFunction;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

public interface StreamConfig {

	static BiFunction<String, String, Properties> streamConf = (uniqueAppId, bootstrapServer) -> {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, Constants.DEFAULT_HOST + ":" + Constants.DEFAULT_PORT);
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, uniqueAppId);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		return props;
	};

}