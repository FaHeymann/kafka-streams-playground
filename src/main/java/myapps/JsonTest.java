package myapps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class JsonTest {
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-json");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    final StreamsBuilder builder = new StreamsBuilder();

    final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
    final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
    final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

    final Consumed<String, JsonNode> consumed = Consumed.with(Serdes.String(), jsonSerde);
    KStream<String, JsonNode> source = builder.stream("streams-json-input", consumed);
    // KStream<String, String> source = builder.stream("streams-json-input", Consumed.with(Serdes.String(), Serdes.String()));

    source.peek((key, value) -> System.out.println("Incoming record - key " +key +" value " + value));

    StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("versionStore"),
            Serdes.String(),
            Serdes.Long()
    );
    builder.addStateStore(storeBuilder);

    final KeyValueMapper<String, JsonNode, KeyValue<String, String>> mapper =
            new KeyValueMapper<>() {
      @Override
      public KeyValue<String, String> apply(String key, JsonNode value) {
        System.out.println("key: " + key.toString() + " value: " + value);
        return new KeyValue<>("highest", value.get("version").toString());
      }
    };

    final KeyValueMapper<String, String, KeyValue<String, String>> stringMapper =
            new KeyValueMapper<>() {
              @Override
              public KeyValue<String, String> apply(String key, String value) {
                System.out.println("key: " + key.toString() + " value: " + value);
                return new KeyValue<>("highest", value);
              }
            };

    //source.map((key, value) -> KeyValue.pair("hello", value.get("version").toString()))
    //        .to("streams-json-output");

    source.transform(() -> new DemoTransformer("versionStore"), "versionStore")
            .peek((key, value) -> System.out.println("after transform: key: " + key.toString() + " value: " + value))
            .to("streams-json-output", Produced.valueSerde(jsonSerde));

    final Topology topology = builder.build();
    System.out.println(topology.describe());

    final KafkaStreams streams = new KafkaStreams(topology, props);

    final CountDownLatch latch = new CountDownLatch(1);

    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
      @Override
      public void run() {
        streams.close();
        latch.countDown();
      }
    });

    try {
      streams.start();
      latch.await();
    } catch (Throwable e) {
      System.exit(1);
    }
    System.exit(0);
  }
}

class DemoTransformer implements Transformer<String, JsonNode, KeyValue<String, JsonNode>> {
  private ProcessorContext processorContext;
  private String stateStoreName;
  private KeyValueStore<String, Long> keyValueStore;

  public DemoTransformer(String stateStoreName) {
    this.stateStoreName = stateStoreName;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.processorContext = processorContext;
    this.keyValueStore = (KeyValueStore) processorContext.getStateStore(stateStoreName);
  }

  @Override
  public KeyValue<String, JsonNode> transform(String key, JsonNode value) {
    Long existingValue = keyValueStore.get(key);
    long version = value.get("version").asLong();
    System.out.println("key: " + key.toString() + " value: " + value);
    System.out.println("key: " + key.toString() + " version: " + value.get("version").asLong());
    System.out.println("existingVersion " + existingValue);
    if (existingValue == null || version > existingValue) {
      System.out.println("got here");
      // processorContext.forward(key, value);
      System.out.println("got here 2");
      try {

        keyValueStore.put(key, version);
      } catch (Throwable e) {
        System.out.println("failed to put key " + key.toString() + " version " + version + e.toString());
      }
      return new KeyValue<>(key, value);
    }

    System.out.println("before return");
    return null;
  }

  @Override
  public void close() {
  }
}

