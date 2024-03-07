package org.course.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {

    private static final Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello World!");

        // Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create the Producer, los tipo del Kafka Producer coinciden con los valores de key y value de las
        // propiedades
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        // Producer record, record que enviamos a kafka, va a ser enviado al topic "demo_java" con valor
        // "Hello World!"
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo_java", "Hello World!");

        producer.send(producerRecord);

        producer.flush();

        //La función close hace una llamada a la funcion flush primero.
        producer.close();
    }
}
