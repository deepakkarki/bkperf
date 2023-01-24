# this is the one that works
java -Dtls.enable=true -Dtls.trustStore.path=/certs/truststore.jks -Dtls.trustStore.passwordPath=/secrets/keystore-password \
    -Dtls.clientAuth=true -Dtls.keyStore.path=/certs/keystore.jks -Dtls.keyStore.passwordPath=/secrets/keystore-password \
    -Dzookeeper.client.secure=true -Dzookeeper.clientCnxnSocket=org.apache.zookeeper.ClientCnxnSocketNetty \
    -Dzookeeper.ssl.keyStore.location=/certs/keystore.jks -Dzookeeper.ssl.keyStore.passwordPath=/secrets/keystore-password \
    -Dzookeeper.ssl.trustStore.location=/certs/truststore.jks -Dzookeeper.ssl.trustStore.passwordPath=/secrets/keystore-password \
    -Dzk.address=wal-1-zk-client:2281 -Dzk.path=/pravega/wal-1-pv/bookkeeper/ledgers -Dduration=600 -Dload=2048 \
    -XX:-MaxFDLimit -jar bkperf-1.0-SNAPSHOT-all.jar

# locally
java -Dzk.address=127.0.0.1:2181 -Dzk.path=/ledgers -Dduration=20 -Dload=100 \
     -XX:-MaxFDLimit -jar ./build/libs/bkperf-1.0-SNAPSHOT-all.jar