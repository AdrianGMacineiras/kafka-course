package org.course.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerWithKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Im a Kafka Producer!");

        // Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create the Producer, los tipo del Kafka Producer coinciden con los valores de key y value de las
        // propiedades
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int j = 0; j < 2; j++) {
            // Al mandar X mensajes de forma rapida, el sistema utiliza Sticky Partitioner por defecto, para mejorar el rendimiento
            // Sticky Partitioner: divide los X mensajes en batches de Y y los manda a una misma particion respectivamente.
            // en vez de utilizar Round Robin, que iria mandando cada mensaje individual a una particion nueva haciendo el proceso
            // de lectura menos eficiente.
            for (int i = 0; i < 10; i++) {

                String topic = "demo_java";
                String key = "id_" + i;
                String value = "Hello World " + i;

                // Producer record, record que enviamos a kafka, va a ser enviado al topic "demo_java" con valor
                // "Hello World!"
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, key, value);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //Executes every time a record successfully sent or an exception is thrown
                        if (e == null) {
                            // the record was succesfully sent
                            log.info("Key: " + key + " | Partition: " + recordMetadata.partition());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        producer.flush();

        //La función close hace una llamada a la funcion flush primero.
        producer.close();
    }
}
