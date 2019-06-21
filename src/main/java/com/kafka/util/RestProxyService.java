package com.kafka.util;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;

import com.kafka.util.RestServices;

public class RestProxyService {
	
	public static RestServices startRestProxy(final KafkaStreams streams, final String host,
			final int port) throws Exception {
		final HostInfo hostInfo = new HostInfo(host, port);
		final RestServices restService = new RestServices(
				streams, hostInfo);
		restService.start(port);
		return restService;
	}

}
