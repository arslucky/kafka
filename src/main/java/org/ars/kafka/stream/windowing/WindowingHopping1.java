package org.ars.kafka.stream.windowing;

import static java.lang.Thread.sleep;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
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
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * @author arsen.ibragimov
 *
 * 15:11:19 [INFO] org.ars.kaf.str.win.WindowingHopping1$Consumer - consumer:start
 * 15:11:19 [INFO] org.ars.kaf.str.win.WindowingHopping1$Producer - producer:start
 * 15:11:20 [INFO] org.ars.kaf.str.win.WindowingHopping1$Producer - send key:0, value:0 15:11:19.822
 * 15:11:21 [INFO] org.ars.kaf.str.win.WindowingHopping1$Producer - send key:0, value:1 15:11:21.141
 * 15:11:22 [INFO] org.ars.kaf.str.win.WindowingHopping1$Producer - send key:0, value:2 15:11:22.156
 * 15:11:23 [INFO] org.ars.kaf.str.win.WindowingHopping1$Producer - producer:stop
 * 15:11:25 [INFO] org.ars.kaf.str.win.WindowingHopping1$Consumer - window period: 15:11:18.000 - 15:11:20.000, key:0, value:0
 * 15:11:25 [INFO] org.ars.kaf.str.win.WindowingHopping1$Consumer - window period: 15:11:19.000 - 15:11:21.000, key:0, value:0
 * 15:11:25 [INFO] org.ars.kaf.str.win.WindowingHopping1$Consumer - window period: 15:11:20.000 - 15:11:22.000, key:0, value:1
 * 15:11:25 [INFO] org.ars.kaf.str.win.WindowingHopping1$Consumer - window period: 15:11:21.000 - 15:11:23.000, key:0, value:1,2
 * 15:11:25 [INFO] org.ars.kaf.str.win.WindowingHopping1$Consumer - window period: 15:11:22.000 - 15:11:24.000, key:0, value:2
 * 15:11:25 [INFO] org.ars.kaf.str.win.WindowingHopping1$Consumer - consumer:stop
 */
public class WindowingHopping1 {

    static Logger log = LogManager.getLogger( WindowingHopping1.class);

    static String topic = WindowingHopping1.class.getSimpleName();

    static final String BOOTSTRAP_SERVERS = "kafka1:9091";

    static final String TIME_FORMAT = "kk:mm:ss.SSS";

    static {
        Configurator.setRootLevel( Level.WARN);
        Configurator.setLevel( "org.apache.kafka.clients.consumer", Level.WARN);
    }

    static class Producer implements Runnable {

        Properties config = new Properties();
        KafkaProducer<Integer, Integer> producer = null;
        final SimpleDateFormat dateFormat = new SimpleDateFormat( TIME_FORMAT);

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
                    ProducerRecord<Integer, Integer> record = new ProducerRecord<>( topic, null, System.currentTimeMillis(), key, i);
                    producer.send( record);
                    log.info( "send key:{}, value:{} {}", key, i, dateFormat.format( new Date( record.timestamp())));
                    sleep( 1000);
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

        final SimpleDateFormat dateFormat = new SimpleDateFormat( TIME_FORMAT);

        public Consumer() {
            String baseDir = String.format( "./kafka-streams/%s", WindowingHopping1.class.getSimpleName());

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
                StreamsBuilder builder = new StreamsBuilder();

                Duration windowSize = Duration.ofMillis( 2000);
                Duration advance = Duration.ofMillis( 1000); // hop

                TimeWindows timeWindow = TimeWindows.ofSizeWithNoGrace( windowSize).advanceBy( advance);

                KStream<Integer, Integer> stream = builder.stream( topic);

                stream.groupByKey().windowedBy( timeWindow).aggregate( () -> "", ( key, value, aggregate) -> {
                    if( aggregate.length() > 0) {
                        aggregate += ",";
                    }
                    aggregate += value;
                    // log.info( "key:{}, value:{}, window values:{}", key, value, aggregate);
                    return aggregate;
                }, Materialized.with( Serdes.Integer(), Serdes.String())).toStream().foreach( ( key, value) -> {
                    log.info( "window period: {} - {}, key:{}, value:{}", dateFormat.format( new Date( key.window().start())), dateFormat.format( new Date( key.window().end())), key.key(), value);
                });

                streams = new KafkaStreams( builder.build(), config);

                Runtime.getRuntime().addShutdownHook( new Thread( streams::close));

                streams.start();
                sleep( 5000);
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
