package io.temporal.bkperf;

import org.apache.bookkeeper.client.*;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZKClientConfig;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class BKPerfTest {

    // System properties required
    //      tls.enable tls.trustStore.path tls.trustStore.passwordPath
    //      tls.clientAuth tls.keyStore.path tls.keyStore.passwordPath
    public static BookKeeper createBkClient() {
        int writeTimeout = (int) Math.ceil(6000 / 1000.0); // hardcoded values, replace later
        int readTimeout = (int) Math.ceil(6000 / 1000.0);
        ClientConfiguration config = new ClientConfiguration()
                .setClientTcpNoDelay(true)
                .setAddEntryTimeout(writeTimeout)
                .setReadEntryTimeout(readTimeout)
                .setGetBookieInfoTimeout(readTimeout)
                .setEnableDigestTypeAutodetection(true)
                .setClientConnectTimeoutMillis(10000)
                .setZkTimeout(10000);

        if (System.getProperty("tls.enable") != null && System.getProperty("tls.enable").equals("true")) {
            config = config.setTLSProvider("OpenSSL");
            config = config.setTLSTrustStore(System.getProperty("tls.trustStore.path"));
            config.setTLSTrustStorePasswordPath(System.getProperty("tls.trustStore.passwordPath"));
            config.setZkEnableSecurity(true);
            if (System.getProperty("tls.clientAuth").equals("true")) {
                config.setTLSClientAuthentication(true);
            }
            if (!System.getProperty("tls.keyStore.path").equals("")) {
                config.setTLSKeyStore(System.getProperty("tls.keyStore.path"));
            }
            if (!System.getProperty("tls.keyStore.passwordPath").equals("")) {
                config.setTLSKeyStorePasswordPath(System.getProperty("tls.keyStore.passwordPath"));
            }
        }

        String path = System.getProperty("zk.path");
        if (path == null){
            path = "";
        }
        String metadataServiceUri = "zk://" + System.getProperty("zk.address") + path;

        config = config.setMetadataServiceUri(metadataServiceUri);

        config = config.setEnsemblePlacementPolicy(DefaultEnsemblePlacementPolicy.class);

        try {
            return new BookKeeper(config);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static LedgerHandle createLedger(BookKeeper bk, String name, byte[] password) {
        try {
            return bk.createLedger(3, 2, 2, DigestType.MAC, password, Collections.singletonMap("name", name.getBytes()));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static LedgerHandle getLedgerReader(BookKeeper bk, long ledgerID, byte[] password) {
        LedgerHandle reader;
        try {
            reader = bk.openLedger(ledgerID, DigestType.MAC, password);
        } catch (BKException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        return reader;
    }

    public static List<Long> listAllLedgers(BookKeeper bk) {
        final List<Long> ledgers = Collections.synchronizedList(new ArrayList<>());
        final CountDownLatch processDone = new CountDownLatch(1);

        bk.getLedgerManager()
                .asyncProcessLedgers((ledgerId, cb) -> {
                            ledgers.add(ledgerId);
                            cb.processResult(BKException.Code.OK, null, null);
                        },
                        (rc, s, obj) -> {
                            processDone.countDown();
                        }, null, BKException.Code.OK, BKException.Code.ReadException);

        try {
            processDone.await(1, TimeUnit.MINUTES);
            return ledgers;
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }
    }

    public static void writeToLedger(LedgerHandle ledgerHandle){
        for(int i = 0; i < 10; i++) {
            byte[] data = new String("message-" + i).getBytes();
            try {
                ledgerHandle.addEntry(data);
            } catch (org.apache.bookkeeper.client.api.BKException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static Enumeration<LedgerEntry> readFromLedger(LedgerHandle ledgerHandle){
        Enumeration<LedgerEntry> entries;
        try {
            entries = ledgerHandle.readEntries(0, ledgerHandle.getLastAddConfirmed());
        } catch (InterruptedException | BKException e) {
            throw new RuntimeException(e);
        }
        return entries;
    }

    public static void deleteLedger(BookKeeper bk, long ledgerId){
        try {
            bk.deleteLedger(ledgerId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        writeReadAndCleanClassic();
    }

    public static void writeReadAndCleanClassic(){
        // Connect to zk. eg. "127.0.0.1:2181"; "wal-1-zk-client:2281"
        BookKeeper bk = createBkClient();

        System.out.println("Creating a ledger...");
        LedgerHandle lh = createLedger(bk, "ledger-1", "password".getBytes());

        System.out.println("Writing to the ledger..." + lh.getId());
        writeToLedger(lh);

        System.out.println("Reading from the ledger...");
        LedgerHandle reader = getLedgerReader(bk, lh.getId(), "password".getBytes());
        Enumeration<LedgerEntry> entries = readFromLedger(reader);

        // print out the entries
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            System.out.println("Successfully read entry " + entry.getEntryId());
        }

        // try to close the reader and writer; delete the ledger
        long ledgerId = lh.getId();
        try {
            lh.close();
            reader.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        deleteLedger(bk, ledgerId);

        // list all ledgers
        System.out.println("Ledgers in the system" + listAllLedgers(bk));

        // close the bookkeeper client
        try {
            bk.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println("Done");
        System.exit(0); // otherwise some background threads get it to hang here.
    }
}
