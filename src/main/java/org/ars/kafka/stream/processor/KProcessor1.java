package org.ars.kafka.stream.processor;

import static java.lang.Thread.sleep;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.processor.UsePartitionTimeOnInvalidTimestamp;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * @author arsen.ibragimov
 *
 */
public class KProcessor1 {

    static Logger log = LogManager.getLogger( KProcessor1.class);

    static String topic = KProcessor1.class.getSimpleName();

    static final String BOOTSTRAP_SERVERS = "kafka1:9091";

    static String sourceNode = topic + "SourceNode";

    static String processorCalculateNode = topic + "ProcessorCalculateNode";

    static String processorEvenNode = topic + "ProcessorEvenNode";

    static String processorOddNode = topic + "ProcessorOddNode";

    static String persistentValues = "persistent-values";

    static {
        Configurator.setRootLevel( Level.WARN);
        Configurator.setLevel( "org.apache.kafka.clients.consumer", Level.WARN);
    }

    static class Producer implements Runnable {

        Properties config = new Properties();
        KafkaProducer<Integer, Integer> producer = null;

        public Producer() {
            config.put( ProducerConfig.CLIENT_ID_CONFIG, "producer1");
            config.put( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            config.put( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
            config.put( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");

            producer = new KafkaProducer<>( config);
        }

        @Override
        public void run() {
            log.info( "producer:start");
            try {
                for( int i = 1; i < 3; i++) {
                    int key = i;
                    ProducerRecord<Integer, Integer> record = new ProducerRecord<>( topic, key, i);
                    producer.send( record);
                    log.info( "send:{}", i);
                }
            } catch( Exception e) {
                e.printStackTrace();
            } finally {
                producer.close();
                log.info( "producer:stop");
            }
        }
    }

    static class ProcessorCalculateNode extends ContextualProcessor<Integer, Integer, Integer, Long> {

        KeyValueStore<Integer, Long> kvStore;

        @Override
        public void init( ProcessorContext<Integer, Long> context) {
            super.init( context);
            kvStore = context().getStateStore( persistentValues);
        }

        @Override
        public void process( Record<Integer, Integer> record) {
            log.info( "key:{}, value:{}", record.key(), record.value());
            Long count = kvStore.get( record.key());
            if( count == null) {
                count = 0l;
            }
            kvStore.put( record.key(), ++count);
            Record<Integer, Long> toForward = record.withValue( count);
            if( count % 2 == 0) {
                context().forward( toForward, processorEvenNode);
            } else {
                context().forward( toForward, processorOddNode);
            }
        }
    }

    static class ProcessorEvenNode extends ContextualProcessor<Integer, Long, Void, Void> {
        @Override
        public void process( Record<Integer, Long> record) {
            log.info( "key:{}, count even:{}", record.key(), record.value());
        }
    }

    static class ProcessorOddNode extends ContextualProcessor<Integer, Long, Void, Void> {
        @Override
        public void process( Record<Integer, Long> record) {
            log.info( "key:{}, count odd:{}", record.key(), record.value());
        }
    }

    static class Consumer implements Runnable {

        Properties config = new Properties();
        KafkaStreams streams = null;

        public Consumer() {
            String baseDir = String.format( "./kafka-streams/%s", KProcessor1.class.getSimpleName());

            config.put( StreamsConfig.APPLICATION_ID_CONFIG, topic + "_client");
            config.put( StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            config.put( StreamsConfig.STATE_DIR_CONFIG, baseDir);
            config.put( StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
            config.put( StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
            // rebalance optimization
            config.put( "internal.leave.group.on.close", "true");
            config.put( ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");
        }

        @Override
        public void run() {
            try {
                log.info( "consumer:start");

                Topology topology = new Topology();

                topology.addSource( AutoOffsetReset.EARLIEST, sourceNode, new UsePartitionTimeOnInvalidTimestamp(), Serdes.Integer().deserializer(), Serdes.Integer().deserializer(), topic);

                topology.addProcessor( processorCalculateNode, new ProcessorSupplier<Integer, Integer, Integer, Long>() {
                    @Override
                    public Processor<Integer, Integer, Integer, Long> get() {
                        return new ProcessorCalculateNode();
                    }
                }, sourceNode);

                StoreBuilder<KeyValueStore<Integer, Long>> store = Stores.keyValueStoreBuilder( Stores.persistentKeyValueStore( persistentValues), Serdes.Integer(), Serdes.Long());
                topology.addStateStore( store, processorCalculateNode);

                topology.addProcessor( processorEvenNode, new ProcessorSupplier<Integer, Long, Void, Void>() {
                    @Override
                    public Processor<Integer, Long, Void, Void> get() {
                        return new ProcessorEvenNode();
                    }
                }, processorCalculateNode);

                topology.addProcessor( processorOddNode, new ProcessorSupplier<Integer, Long, Void, Void>() {
                    @Override
                    public Processor<Integer, Long, Void, Void> get() {
                        return new ProcessorOddNode();
                    }
                }, processorCalculateNode);

                streams = new KafkaStreams( topology, config);

                Runtime.getRuntime().addShutdownHook( new Thread( streams::close));

                streams.start();
                sleep( 1500);
            } catch( Exception e) {
                e.printStackTrace();
            } finally {
                streams.close();
                log.info( "consumer:stop");
            }
        }
    }

    public static void main( String[] args) {
        try {
            Thread thread1 = new Thread( new Producer());
            Thread thread2 = new Thread( new Consumer());

            thread1.start();
            sleep( 500);
            thread2.start();
        } catch( Exception e) {
            e.printStackTrace();
        }
    }
}
