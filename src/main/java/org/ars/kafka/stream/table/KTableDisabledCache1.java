package org.ars.kafka.stream.table;

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
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * @author arsen.ibragimov
 *
 * KTable output with disabled cache
 * disabling the cache will send all records downstream, including duplicate keys
 *
 * 13:38:12 [INFO] org.ars.kaf.str.tab.KTableDisabledCache1$Producer - producer:start
 * 13:38:12 [INFO] org.ars.kaf.str.tab.KTableDisabledCache1$Producer - send key:0, value:0
 * 13:38:12 [INFO] org.ars.kaf.str.tab.KTableDisabledCache1$Producer - send key:0, value:1
 * 13:38:12 [INFO] org.ars.kaf.str.tab.KTableDisabledCache1$Producer - send key:0, value:2
 * 13:38:12 [INFO] org.ars.kaf.str.tab.KTableDisabledCache1$Producer - producer:stop
 * 13:38:13 [INFO] org.ars.kaf.str.tab.KTableDisabledCache1$Consumer - consumer:start
 * 13:38:13 [INFO] org.ars.kaf.str.tab.KTableDisabledCache1$Consumer - store name:queryable-store-name
 * 13:38:14 [INFO] org.ars.kaf.str.tab.KTableDisabledCache1$Consumer - table key:0, value:0
 * 13:38:14 [INFO] org.ars.kaf.str.tab.KTableDisabledCache1$Consumer - table key:0, value:1
 * 13:38:14 [INFO] org.ars.kaf.str.tab.KTableDisabledCache1$Consumer - table key:0, value:2
 * 13:38:17 [INFO] org.ars.kaf.str.tab.KTableDisabledCache1$Consumer - consumer:stop
 */
public class KTableDisabledCache1 {

    static Logger log = LogManager.getLogger( KTableDisabledCache1.class);

    static String topic = KTableDisabledCache1.class.getSimpleName();

    static final String BOOTSTRAP_SERVERS = "kafka1:9091";

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
                    int key = 0;
                    ProducerRecord<Integer, Integer> record = new ProducerRecord<>( topic, key, i);
                    producer.send( record);
                    log.info( "send key:{}, value:{}", key, i);
                }
            } catch( Exception e) {
                e.printStackTrace();
            } finally {
                producer.close();
                log.info( "producer:stop");
            }
        }
    }

    static class Consumer implements Runnable {

        Properties config = new Properties();
        KafkaStreams streams = null;

        public Consumer() {
            String baseDir = String.format( "./kafka-streams/%s", KTableDisabledCache1.class.getSimpleName());

            config.put( StreamsConfig.APPLICATION_ID_CONFIG, topic + "_client");
            config.put( StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            config.put( StreamsConfig.STATE_DIR_CONFIG, baseDir);
            config.put( StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
            config.put( StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
            // rebalance optimization
            config.put( "internal.leave.group.on.close", "true");
            config.put( ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");
            // config.put( StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000"); // forwards the record “downstream” to its child nodes, 30 sec by default. Doesn't matter when cache buffer disabled
            // config.put( StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0"); // disable caching, deprecated
            config.put( StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0"); // disable caching
        }

        @Override
        public void run() {
            try {
                log.info( "consumer:start");
                StreamsBuilder builder = new StreamsBuilder();

                KTable<Integer, Integer> table = builder.table( topic, Materialized.as( "queryable-store-name"));

                table.toStream().foreach( ( key, value) -> {
                    log.info( "table key:{}, value:{}", key, value);
                });

                final String queryableStoreName = table.queryableStoreName();

                log.info( "store name:{}", queryableStoreName);

                streams = new KafkaStreams( builder.build(), config);

                Runtime.getRuntime().addShutdownHook( new Thread( streams::close));

                streams.start();
                sleep( 2000);
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
            log.info( "start");
            Thread thread1 = new Thread( new Producer());
            Thread thread2 = new Thread( new Consumer());

            thread1.start();
            sleep( 1000);
            thread2.start();
        } catch( Exception e) {
            e.printStackTrace();
        } finally {
            log.info( "end");
        }
    }
}
