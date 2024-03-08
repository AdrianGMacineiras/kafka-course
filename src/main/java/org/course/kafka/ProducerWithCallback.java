package org.course.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Im a Kafka Producer!");

        // Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        properties.setProperty("batch.size", "400");

        // Create the Producer, los tipo del Kafka Producer coinciden con los valores de key y value de las
        // propiedades
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);


        for (int j = 0; j < 10; j++) {
            // Al mandar X mensajes de forma rapida, el sistema utiliza Sticky Partitioner por defecto, para mejorar el rendimiento
            // Sticky Partitioner: divide los X mensajes en batches de Y y los manda a una misma particion respectivamente.
            // en vez de utilizar Round Robin, que iria mandando cada mensaje individual a una particion nueva haciendo el proceso
            // de lectura menos eficiente.
            for (int i = 0; i < 30; i++) {
                // Producer record, record que enviamos a kafka, va a ser enviado al topic "demo_java" con valor
                // "Hello World!"
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("demo_java", "Hello World! " + i);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //Executes every time a record successfully sent or an exception is thrown
                        if (e == null) {
                            // the record was succesfully sent
                            log.info("Received new metadata \n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp()
                            );
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
