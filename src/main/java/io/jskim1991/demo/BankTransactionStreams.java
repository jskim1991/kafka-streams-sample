package io.jskim1991.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Instant;
import java.util.Properties;

public class BankTransactionStreams {

    // kafka-topics.sh --zookeeper localhost:2181 --create --topic bank-balance --partitions 1 --replication-factor 1

    /*
     kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bank-balance --from-beginning \
       --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true \
       --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
       --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
     */
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final Serializer<JsonNode> jsonNodeSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonNodeDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonNodeSerializer, jsonNodeDeserializer);

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, JsonNode> bankTransactions = streamsBuilder.stream("bank-transactions", Consumed.with(Serdes.String(), jsonSerde));

        ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put("count", 0);
        initialBalance.put("balance", 0);
        initialBalance.put("time", Instant.ofEpochMilli(0L).toString());

        KTable<String, JsonNode> bankBalance = bankTransactions
                .groupByKey()
                .aggregate(
                        () -> initialBalance,
                        (key, tx, balance) -> newBalance(tx, balance),
                        Materialized.with(Serdes.String(), jsonSerde)
                );

        bankBalance.toStream().to("bank-balance", Produced.with(Serdes.String(), jsonSerde));

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), config);
        streams.start();

        System.out.println(streams.toString());

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    private static JsonNode newBalance(JsonNode tx, JsonNode balance) {
        ObjectNode b = JsonNodeFactory.instance.objectNode();
        b.put("count", balance.get("count").asInt() + 1);
        b.put("balance", balance.get("balance").asInt() + tx.get("amount").asInt());
        b.put("time", Instant.ofEpochMilli(Math.max(Instant.parse(balance.get("time").asText()).toEpochMilli(), Instant.parse(tx.get("time").asText()).toEpochMilli())).toString());
        return b;
    }
}
