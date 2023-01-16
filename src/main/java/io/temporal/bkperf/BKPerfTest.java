package io.temporal.bkperf;

import org.apache.bookkeeper.client.*;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.ReadHandle;
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

    public static LedgerHandleAdv createLedgerAdv(BookKeeper bk, long ledgerId){
        long base = 1000000000;
        ledgerId += base;
        System.out.println("Creating a ledger with ID : " + ledgerId);
        LedgerHandleAdv lh;
        try {
            lh = (LedgerHandleAdv) bk.createLedgerAdv(ledgerId, 3, 3, 2,
                    DigestType.CRC32, "password".getBytes(), Collections.singletonMap("name", "name".getBytes()));
        } catch (Exception e){
            throw new RuntimeException(e);
        }
        return lh;
    }

    public static LedgerHandle getLedgerReader(BookKeeper bk, long ledgerID) {
        LedgerHandle reader;
        try {
            reader = bk.openLedger(ledgerID, DigestType.MAC, "password".getBytes());
        } catch (BKException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        return reader;
    }

    // write "entries" number of 1KB entries to the ledger
    public static void writeToLedger(LedgerHandleAdv lh, long entries){
        for(long i = 0; i < entries; i++) {
            // average byte size
            byte[] data = new String("message - " + i).repeat(1000).getBytes();
            try {
                lh.addEntry(i, data);
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
        owrcScenario(Integer.parseInt(System.getProperty("ledgerID")));
    }

    // open write read close scenario
    public static void owrcScenario(long ledgerId){ //TODO : wrap in a timer
        BookKeeper bk = createBkClient();

        LedgerHandleAdv lh = createLedgerAdv(bk, ledgerId);
        long itemsToWrite = 25;
        writeToLedger(lh, itemsToWrite); //TODO : wrap in a timer

        LedgerHandle reader = getLedgerReader(bk, lh.getId());
        readFromLedger(reader); //TODO : wrap in a timer


        // try to close the reader and writer; delete the ledger
        try {
            lh.close();
            reader.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        deleteLedger(bk, ledgerId);

        // close the bookkeeper client
        try {
            bk.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // unused for now
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
}
