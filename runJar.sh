java -Dtls.enable=true -Dtls.trustStore.path=/certs/truststore.jks -Dtls.trustStore.passwordPath=/secrets/keystore-password \
    -Dtls.clientAuth=true -Dtls.keyStore.path=/certs/keystore.jks -Dtls.keyStore.passwordPath=/secrets/keystore-password \
    -Dzk.address=wal-1-zk-client:2281 \
    -jar bkperf-1.0-SNAPSHOT-all.jar


# this is the one that works
java -Dtls.enable=true -Dtls.trustStore.path=/certs/truststore.jks -Dtls.trustStore.passwordPath=/secrets/keystore-password \
    -Dtls.clientAuth=true -Dtls.keyStore.path=/certs/keystore.jks -Dtls.keyStore.passwordPath=/secrets/keystore-password \
    -Dzookeeper.client.secure=true -Dzookeeper.clientCnxnSocket=org.apache.zookeeper.ClientCnxnSocketNetty \
    -Dzookeeper.ssl.keyStore.location=/certs/keystore.jks -Dzookeeper.ssl.keyStore.passwordPath=/secrets/keystore-password \
    -Dzookeeper.ssl.trustStore.location=/certs/truststore.jks -Dzookeeper.ssl.trustStore.passwordPath=/secrets/keystore-password \
    -Dzk.address=wal-1-zk-client:2281 -Dzk.path=/wal-1/bookkeeper/ledgers \
    -jar bkperf-1.0-SNAPSHOT-all.jar

