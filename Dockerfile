FROM confluentinc/cp-server-connect:7.6.0 //use 8.1.0

COPY dummy-source-connector-plugin/lib/ /usr/share/java/dummy-source-connector/