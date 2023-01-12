package io.temporal.bkperf;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class BKPerfTest {

    public static BookKeeper createBkClient(String zkConnectionString) {
        try {
            return new BookKeeper(zkConnectionString);
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

    public static void main(String[] args) {
        // Connect to zk (and register metadata?)
        BookKeeper bk = createBkClient("127.0.0.1:2181");

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

        System.out.println("Current ledgers in the system: "+listAllLedgers(bk));

        System.out.println("Done");
        System.exit(0); //for some reason gradle does not complete the run otherwise.
    }
}
