# kafka-course
# Iniciar Zookeeper
    /opt/homebrew/Cellar/kafka/3.7.0/bin/zookeeper-server-start /opt/homebrew/etc/zookeeper/zoo.cfg
# Iniciar servidor
    /opt/homebrew/bin/kafka-server-start /opt/homebrew/etc/kafka/server.properties
# Consumer (No me devuelve nada, en el momento que cierro el servidor se queja de fallo de conexión, asi que sí que se conecta al servidor)
    kafka-console-consumer --topic demo_java --from-beginning --bootstrap-server localhost:9092

# Comando que me dio ChatGpt
    kcat -C -b localhost:9092 -t demo_java -o beginning -e -f 'Topic %t[%p], offset: %o, key: %k, payload: %s bytes: %S\n'

# Retorno del comando
    Topic demo_java[0], offset: 0, key: , payload: Hello World! bytes: 12
    Topic demo_java[0], offset: 1, key: , payload: Hello World! bytes: 12
    Topic demo_java[0], offset: 2, key: , payload: Hello World! bytes: 12
    Topic demo_java[0], offset: 3, key: , payload: Hello World! bytes: 12
    % Reached end of topic demo_java [0] at offset 4
    % Reached end of topic demo_java [1] at offset 0
    Topic demo_java[2], offset: 0, key: , payload: Hello World! bytes: 12
    Topic demo_java[2], offset: 1, key: , payload: Hello World! bytes: 12
    Topic demo_java[2], offset: 2, key: , payload: Hello World! bytes: 12
    % Reached end of topic demo_java [2] at offset 3: exiting

