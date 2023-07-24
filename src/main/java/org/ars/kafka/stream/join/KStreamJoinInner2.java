package org.ars.kafka.stream.join;

import static java.lang.Thread.currentThread;
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
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * @author arsen.ibragimov
 *
 *         Inner join
 */
public class KStreamJoinInner2 {

    static Logger log = LogManager.getLogger( KStreamJoinInner2.class);

    static String topic = KStreamJoinInner2.class.getSimpleName();

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
            log.info( String.format( "%s producer:start", currentThread().getName()));
            try {
                int i = 1;
                ProducerRecord<Integer, Integer> record = new ProducerRecord<>( topic, i, i);
                producer.send( record);
                log.info( String.format( "%s send:%d", currentThread().getName(), i));
            } catch( Exception e) {
                e.printStackTrace();
            } finally {
                producer.close();
                log.info( String.format( "%s producer:stop", currentThread().getName()));
            }
        }
    }

    static class MyFixedKeyProcessor implements FixedKeyProcessor<Integer, Integer, Integer> {

        private static final String TIME_FORMAT = "kk:mm:ss.SSS";
        SimpleDateFormat dateFormat = new SimpleDateFormat( TIME_FORMAT);
        int index;

        public MyFixedKeyProcessor( int index) {
            this.index = index;
        }

        @Override
        public void process( FixedKeyRecord<Integer, Integer> record) {
            log.info( String.format( "stream%d key:%d, value:%d, date:%s", this.index, record.key(), record.value(), dateFormat.format( new Date( record.timestamp()))));
        }

    }

    static class Consumer implements Runnable {

        Properties config = new Properties();
        KafkaStreams streams = null;

        public Consumer() {
            String baseDir = String.format( "./kafka-streams/%s", KStreamJoinInner2.class.getSimpleName());

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
                KStream<Integer, Integer> stream1 = builder.stream( topic);
                KStream<Integer, Integer> stream2 = builder.stream( topic);

                stream1.processValues( new FixedKeyProcessorSupplier<Integer, Integer, Integer>() {
                    @Override
                    public FixedKeyProcessor<Integer, Integer, Integer> get() {
                        return new MyFixedKeyProcessor( 1);
                    }
                });

                stream2.processValues( new FixedKeyProcessorSupplier<Integer, Integer, Integer>() {
                    @Override
                    public FixedKeyProcessor<Integer, Integer, Integer> get() {
                        return new MyFixedKeyProcessor( 2);
                    }
                });

                KStream<Integer, Integer> streamJoin = stream1.join( stream2, ( val1, val2) -> val1 + val2, JoinWindows.ofTimeDifferenceWithNoGrace( Duration.ofMillis( 50000)));
                // -------------------------------
                streamJoin.foreach( ( key, value) -> {
                    log.info( String.format( "joined key:%d, sum:%s", key, value));
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
