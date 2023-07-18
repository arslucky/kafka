package org.ars.kafka.stream;

import static java.lang.Thread.sleep;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
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
 *         Simple KStream output and transform values with storing, statefull
 */
public class KStreamTransformValues1 {

    static Logger log = LogManager.getLogger( KStreamTransformValues1.class);

    static String topic = KStreamTransformValues1.class.getSimpleName();

    static final String BOOTSTRAP_SERVERS = "kafka1:9091";

    static final String STATE_STORE = topic + "_client";

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
                for( int i = 0; i < 3; i++) {
                    ProducerRecord<Integer, Integer> record = new ProducerRecord<>( topic, i);
                    producer.send( record);
                    log.info( "send:" + i);
                }
            } catch( Exception e) {
                e.printStackTrace();
            } finally {
                producer.close();
                log.info( "producer:stop");
            }
        }
    }

    static class MyValueTransformer implements ValueTransformer<Integer, Integer> {

        private KeyValueStore<Integer, Integer> stateStore;
        private ProcessorContext context;

        @Override
        public void init( ProcessorContext context) {
            this.context = context;
            this.stateStore = this.context.getStateStore( STATE_STORE);
            // punctuate each second, can access this.state
            // context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, new Punctuator(..));
        }

        @Override
        public Integer transform( Integer value) {
            if( this.stateStore.get( 0) != null) {
                value += this.stateStore.get( 0);
            }
            this.stateStore.put( 0, value);
            return value;
        }

        @Override
        public void close() {
            // can access this.state
        }
    }

    static class Consumer implements Runnable {

        Properties config = new Properties();
        KafkaStreams streams = null;

        public Consumer() {
            String baseDir = String.format( "./kafka-streams/%s", KStreamTransformValues1.class.getSimpleName());

            config.put( StreamsConfig.APPLICATION_ID_CONFIG, topic + "_client"); // STATE_STORE
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
                StreamsBuilder builder = new StreamsBuilder();
                KStream<Integer, Integer> stream = builder.stream( topic);

                stream.foreach( ( key, value) -> {
                    log.info( String.format( "key:%d, value:%d", key, value));
                });
                // -------------------------------
                // create store
                StoreBuilder<KeyValueStore<Integer, Integer>> keyValueStoreBuilder = Stores.keyValueStoreBuilder( Stores.persistentKeyValueStore( STATE_STORE), Serdes.Integer(), Serdes.Integer());
                // add store
                builder.addStateStore( keyValueStoreBuilder);

                @SuppressWarnings( "deprecation")
                KStream<Integer, Integer> stream1 = stream.transformValues( new ValueTransformerSupplier<Integer, Integer>() {
                    @Override
                    public ValueTransformer<Integer, Integer> get() {
                        return new MyValueTransformer();
                    }
                }, STATE_STORE);

                // -------------------------------
                stream1.foreach( ( key, value) -> {
                    log.info( String.format( "key:%d, sum:%s", key, value));
                });

                streams = new KafkaStreams( builder.build(), config);

                Runtime.getRuntime().addShutdownHook( new Thread( streams::close));

                streams.start();
                sleep( 1000);
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
