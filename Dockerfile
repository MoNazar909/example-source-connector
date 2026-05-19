FROM confluentinc/cp-server-connect:8.1.0

COPY dummy-source-connector-plugin/lib/ /usr/share/java/dummy-source-connector/
