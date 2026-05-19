FROM confluentinc/cp-server-connect:8.1.0

COPY generic-http-sink-connector-plugin/lib/ /usr/share/java/generic-http-sink-connector/
