package io.temporal.bkperf;

import org.apache.bookkeeper.client.*;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class BKPerfTest {

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

    public static LedgerHandleAdv createLedgerAdv(BookKeeper bk) {
        long base = 1000000;
        LedgerHandleAdv lh;
        try {
            lh = (LedgerHandleAdv) bk.createLedgerAdv(3, 3, 2,
                    DigestType.CRC32, "password".getBytes(), Collections.singletonMap("name", "name".getBytes()));
            System.out.println("Created a ledger with ID : " + lh.getId());
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
    public static long[] writeToLedger(LedgerHandleAdv lh, long entries){
        long writeTimes[] = new long[(int)entries];
        for(long i = 0; i < entries; i++) {
            // average byte size = 1KB
            byte[] data = ("message - " + i).repeat(100).getBytes();
            try {
                long beforeWrite = Instant.now().toEpochMilli();
                lh.addEntry(i, data);
                writeTimes[(int)i] = Instant.now().toEpochMilli() - beforeWrite;
            } catch (org.apache.bookkeeper.client.api.BKException | InterruptedException e) {
                System.out.println("Error writing entry: " + i);
            }
        }
        return writeTimes;
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
            System.out.println("Unable to delete ledger:"+ ledgerId);
        }
    }

    public static void main(String[] args) {
        System.out.println("Starting...");
        int load = Integer.parseInt(System.getProperty("load")); // concurrency level
        BookKeeper bk = createBkClient();

        CountDownLatch countDownLatch = new CountDownLatch(load);
        BKMetric[] metrics = new BKMetric[load];
        for (int i =0; i < load; i++){
            long j = i; // because not sure how closures work in java
            new Thread( () -> {
                System.out.println("Starting ledger #" + j);
                BKMetric metric = owrcScenario(bk, 10);
                metrics[(int)j] = metric;
                countDownLatch.countDown();
            }).start();
        }
        try {
            countDownLatch.await();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        BKMetric metric = BKMetric.summarize(metrics);
        try{
            metric.write("metrics.log", load+", ");
        } catch (Exception e) {
            System.out.println("Error writing output to file");
        }

        // close the bookkeeper client
        try {
            bk.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println("Done...");
    }

    // open write read close scenario.
    public static BKMetric owrcScenario(BookKeeper bk, long writeCount){
        long startTime = Instant.now().toEpochMilli();
        BKMetric metric = new BKMetric();

        // create ledger and record the time
        long beforeCreate = Instant.now().toEpochMilli();
        LedgerHandleAdv lh = createLedgerAdv(bk); // createLedgerAdv(bk, ledgerId);
        long ledgerId = lh.getId();
        metric.createTime = Instant.now().toEpochMilli() - beforeCreate;

        // write 'writeCount' entries (each ~1KB) to the ledger
        long beforeWrite = Instant.now().toEpochMilli();
        metric.writeTimes = writeToLedger(lh, writeCount);
        metric.writeTime = Instant.now().toEpochMilli() - beforeWrite;

        // read the ledger and record the time
        LedgerHandle reader = getLedgerReader(bk, lh.getId());
        long beforeRead = Instant.now().toEpochMilli();
        Enumeration<LedgerEntry> entries = readFromLedger(reader);
        metric.readTime = Instant.now().toEpochMilli() - beforeRead;

        // sanity check the values read
        long itemsRead = readSize(entries);
        if (itemsRead != writeCount){
            System.out.printf("Ledger: %d - only %d items read. %d expected.\n", ledgerId, itemsRead, writeCount);
        }

        // try to close the reader and writer; delete the ledger
        try {
            lh.close();
            reader.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        metric.totalTime = Instant.now().toEpochMilli() - startTime;

        deleteLedger(bk, ledgerId);

        return metric;
    }

    private static long readSize(Enumeration<LedgerEntry> items){
        long ic = 0;
        while(items.hasMoreElements()) {
            items.nextElement();
            ic++;
        }
        return ic;
    }

}


class BKMetric {
    public long createTime;
    public long writeTime;
    public long readTime;
    public long totalTime;

    public long[] writeTimes;

    public BKMetric(){
        this.createTime = 0;
        this.writeTime = 0;
        this.readTime = 0;
        this.totalTime = 0;
    }

    public static BKMetric summarize(BKMetric[] metrics){
        BKMetric bk = new BKMetric();
        bk.writeTimes = new long[metrics[0].writeTimes.length];
        for (BKMetric metric : metrics) {
            bk.createTime += metric.createTime;
            bk.writeTime += metric.writeTime;
            bk.readTime += metric.readTime;
            bk.totalTime += metric.totalTime;
            for(int i =0; i< metric.writeTimes.length; i++){
                bk.writeTimes[i] += metric.writeTimes[i];
            }
        }
        bk.createTime /= metrics.length;
        bk.writeTime /= metrics.length;
        bk.readTime /= metrics.length;
        bk.totalTime /= metrics.length;
        for(int i =0; i< bk.writeTimes.length; i++){
            bk.writeTimes[i] /= metrics.length;
        }
        return bk;
    }

    // write the metrics to a file
    public void write(String fileName, String prefix) throws IOException {
        String str = String.format("%d, %d, %d, %d\n", createTime, writeTime, readTime, totalTime);
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true));
        writer.append(prefix)
                .append(str)
                .append(Arrays.toString(this.writeTimes))
                .append("\n");
        writer.flush();
        writer.close();
    }
}