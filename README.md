# kafka-course
# Iniciar Zookeeper
    /opt/homebrew/Cellar/kafka/3.7.0/bin/zookeeper-server-start /opt/homebrew/etc/zookeeper/zoo.cfg
# Iniciar servidor
    /opt/homebrew/bin/kafka-server-start /opt/homebrew/etc/kafka/server.properties
# Problema
Consumer de código al hacer el "consumer.poll" me devuelve un record vacío, las propiedades
de la configuración son las correctas y se asignan bien.

