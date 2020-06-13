package com.rurocker.demo.kafkasecurity.stream;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

public class FaveColor {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fave-color");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.109:9092");
		props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		StreamsBuilder builder = new StreamsBuilder();

		final CountDownLatch latch = new CountDownLatch(1);

		// ricky, red
		// theo, green
		// sophie, blue
		KStream<String, String> textlines = builder.stream("favourite-color-input");
		textlines = textlines.filter((key, value) -> value.contains(","))
			.selectKey((key, value) ->  value.split(",")[0])
			.mapValues( value -> value.split(",")[1].toLowerCase())
			.filter((user,colour) -> Arrays.asList("red","green","blue").contains(colour));
		
		String transit = "user-keys-colours-compact";
		textlines.to(transit);

		String transit2 = "user-keys-colours";
		textlines.to(transit2, Produced.with(Serdes.String(), Serdes.String()));

		KTable<String, String> tableCompact = builder.table(transit);
		KTable<String, Long> countCompact = tableCompact
			.groupBy((user,color) -> new KeyValue<>(color, color))
			.count(Named.as("CountByColorsCompact"));
		
		countCompact.toStream().to("favourite-colour-output-compact", Produced.with(Serdes.String(), Serdes.Long()));
		
		KTable<String, String> table = builder.table(transit2);
		KTable<String, Long> count = table
			.groupBy((user,color) -> new KeyValue<>(color, color))
			.count(Named.as("CountByColors"));
		count.toStream().to("favourite-colour-output", Produced.with(Serdes.String(), Serdes.Long()));
		
		final KafkaStreams streams = new KafkaStreams(builder.build(), props);

		// attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});

//		streams.cleanUp();
		try {
			streams.start();
			latch.await();
		} catch (final Throwable e) {
			System.exit(1);
		}
		System.exit(0);

	}
}
