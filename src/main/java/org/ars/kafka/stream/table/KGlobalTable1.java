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
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * @author arsen.ibragimov
 *
 */
public class KGlobalTable1 {

    static Logger log = LogManager.getLogger( KGlobalTable1.class);

    static String topic_product = KGlobalTable1.class.getSimpleName() + "Product";

    static String topic_sale = KGlobalTable1.class.getSimpleName() + "Sale";

    static final String BOOTSTRAP_SERVERS = "kafka1:9091";

    static {
        Configurator.setRootLevel( Level.WARN);
        Configurator.setLevel( "org.apache.kafka.clients.consumer", Level.WARN);
    }

    static class Producer implements Runnable {

        Properties config1 = new Properties();
        Properties config2 = new Properties();
        KafkaProducer<Integer, String> producerProduct = null;
        KafkaProducer<Integer, Integer> producerSale = null;

        public Producer() {
            config1.put( ProducerConfig.CLIENT_ID_CONFIG, "producer_product");
            config1.put( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            config1.put( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
            config1.put( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

            producerProduct = new KafkaProducer<>( config1);

            config2.put( ProducerConfig.CLIENT_ID_CONFIG, "producer_sale");
            config2.put( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            config2.put( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
            config2.put( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");

            producerSale = new KafkaProducer<>( config2);
        }

        @Override
        public void run() {
            log.info( "producer:start");
            try {
                for( int i = 1; i < 4; i++) {
                    int keySale = i;
                    int keyProduct = i * 10;
                    String value = "product " + keyProduct;

                    ProducerRecord<Integer, String> record = new ProducerRecord<>( topic_product, keyProduct, value);
                    producerProduct.send( record);
                    log.info( "send product key:{}, value:{}", keyProduct, value);

                    ProducerRecord<Integer, Integer> recordSale = new ProducerRecord<>( topic_sale, keySale, keyProduct);
                    producerSale.send( recordSale);
                    log.info( "send sale key:{}, key product:{}", keySale, keyProduct);
                }
            } catch( Exception e) {
                e.printStackTrace();
            } finally {
                producerProduct.close();
                producerSale.close();
                log.info( "producer:stop");
            }
        }
    }

    static class Consumer implements Runnable {

        Properties config = new Properties();
        KafkaStreams streams = null;

        public Consumer() {
            String baseDir = String.format( "./kafka-streams/%s", KGlobalTable1.class.getSimpleName());

            config.put( StreamsConfig.APPLICATION_ID_CONFIG, topic_sale + "_client");
            config.put( StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            config.put( StreamsConfig.STATE_DIR_CONFIG, baseDir);
            config.put( StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
            config.put( StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
            config.put( StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1"); // default 1
            // rebalance optimization
            config.put( "internal.leave.group.on.close", "true");
            config.put( ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");
        }

        @Override
        public void run() {
            try {
                log.info( "consumer:start");
                StreamsBuilder builder = new StreamsBuilder();

                KStream<Integer, Integer> stream = builder.stream( topic_sale);
                GlobalKTable<Integer, String> globalTable = builder.globalTable( topic_product, Materialized.with( Serdes.Integer(), Serdes.String()));

                stream.foreach( ( key, value) -> {
                    log.info( "sale key:{}, product key:{}", key, value);
                });

                //@formatter:off
                KStream<Integer, String> streamJoined = stream.leftJoin( globalTable, ( streamKey, streamValue) -> streamValue /* streamValue == globalTable.key */,
                  (streamValue, globalTableValue) -> String.format( "product:%s", globalTableValue));
                //@formatter:on

                streamJoined.foreach( ( key, value) -> {
                    log.info( "streamJoined key:{}, value:{}", key, value);
                });

                streams = new KafkaStreams( builder.build(), config);

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
