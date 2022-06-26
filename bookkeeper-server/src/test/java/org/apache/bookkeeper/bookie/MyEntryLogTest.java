/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.PortManager;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.*;

import static org.junit.Assert.assertEquals;

@RunWith(value = Enclosed.class)
public class MyEntryLogTest {
    final static List<File> tempDirs = new ArrayList<File>();
    private static File rootDir;
    private static File curDir;
    private static ServerConfiguration conf;
    private static LedgerDirsManager dirsMgr;
    private static EntryLogger entryLogger;
    private static EntryLogManagerForSingleEntryLog entryLogManager;
    private static List<EntryLogger.EntryLogListener> listeners;

    private static EntryLogger.RecentEntryLogsStatus recentlyCreatedEntryLogsStatus;
    private static EntryLoggerAllocator entryLoggerAllocator;


    @RunWith(Parameterized.class)
    public static class CreateLogTest {
        private int numberLogCreate;
        private int ledgerid;
        private int expected;

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][] {
                    { 0,1},
                    { 1,1},
                    { 4,1},
                    { 0,2},
                    { 1,2},
                    { 4,2},
            });
        }

        public CreateLogTest(int numberLogCreate, int ledgerid) throws Exception {
            this.numberLogCreate=numberLogCreate;
            this.ledgerid=ledgerid;
            this.expected = getOracleTestCreateNewLog(this.numberLogCreate);
            setUp();
        }

        @Test
        public void testCreateNewLog() throws Exception {

            ByteBuf entry;

            for(int i=0; i<this.numberLogCreate; i++){
                entry = generateEntry(this.ledgerid, i);
                entryLogManager.addEntry(this.ledgerid, entry,true);
                entryLogManager.createNewLog(EntryLogger.UNASSIGNED_LEDGERID);
            }

            assertEquals(expected, entryLogManager.getCurrentLogId());
        }

        public int getOracleTestCreateNewLog(int numberExecution){
            if(numberExecution == 0 )
                return EntryLogger.UNINITIALIZED_LOG_ID;
            return numberExecution;
        }
    }

    @RunWith(Parameterized.class)
    public static class WriteLogTest {
        private long ledgeridToSearch;
        private List<Long> listledgerid;
        private boolean expected;

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][] {
                    { Arrays.asList(1L,2L,3L,4L),1L},
                    { Arrays.asList(1L,2L,3L,4L),2L},
                    { Arrays.asList(1L,2L,3L,4L),4L},
                    { Arrays.asList(1L,2L,3L,4L),5L}
            });
        }

        public WriteLogTest(List<Long> listledgerid, long ledgeridToSearch) throws Exception {
            this.listledgerid=listledgerid;
            this.ledgeridToSearch=ledgeridToSearch;
            this.expected = getOracleTestWriteLog(listledgerid,ledgeridToSearch);
            setUp();
        }

        @Test
        public void testWriteLog() throws Exception {
            ByteBuf entry;

            for(long i  : this.listledgerid){
                entry = generateEntry(i, 1);
                entryLogManager.addEntry(i, entry,true);
            }

            entryLogManager.createNewLog(EntryLogger.UNASSIGNED_LEDGERID);
            EntryLogMetadata meta = entryLogger.extractEntryLogMetadataFromIndex(0L);
            assertEquals(this.expected,meta.getLedgersMap().containsKey(ledgeridToSearch));

        }
        public boolean getOracleTestWriteLog(List<Long> listledgerid, long ledgeridToSearch){
           return listledgerid.contains(ledgeridToSearch);
        }
    }

    @RunWith(Parameterized.class)
    public static class WriteMultipleLogTest {
        private long ledgeridToSearch;
        private List<Long> listledgerid;
        private long positionLog;
        private boolean expected;

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][] {
                    { Arrays.asList(1L,2L,3L,4L),1L,0L},
                    { Arrays.asList(1L,2L,3L,4L),2L,1L},
                    { Arrays.asList(1L,2L,3L,4L),4L,3L},
                    { Arrays.asList(1L,2L,3L,4L),5L,4L}
            });
        }

        public WriteMultipleLogTest(List<Long> listledgerid, long ledgeridToSearch,long positionLog) throws Exception {
            this.listledgerid=listledgerid;
            this.ledgeridToSearch=ledgeridToSearch;
            this.positionLog=positionLog;
            this.expected = getOracleTestWriteMultipleLog(listledgerid,ledgeridToSearch,positionLog);
            setUp();
        }

        @Test
        public void testWriteLog() throws Exception {
            ByteBuf entry;

            for(long i  : this.listledgerid){
                entry = generateEntry(i, 1);
                entryLogManager.addEntry(i, entry,true);
                entryLogManager.createNewLog(EntryLogger.UNASSIGNED_LEDGERID);
            }

            entryLogManager.createNewLog(EntryLogger.UNASSIGNED_LEDGERID);
            EntryLogMetadata meta = entryLogger.extractEntryLogMetadataFromIndex(this.positionLog);
            assertEquals(this.expected,meta.getLedgersMap().containsKey(ledgeridToSearch));

        }
        public boolean getOracleTestWriteMultipleLog(List<Long> listledgerid, long ledgeridToSearch, long positionLog){
            return listledgerid.contains(ledgeridToSearch) && (positionLog==ledgeridToSearch-1);
        }
    }


    static File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tempDirs.add(dir);
        return dir;
    }

    public static ServerConfiguration getServerConfiguration() {
        ServerConfiguration confReturn = new ServerConfiguration();
        confReturn.setTLSEnabledProtocols("TLSv1.2,TLSv1.1");
        confReturn.setJournalFlushWhenQueueEmpty(true);
        confReturn.setJournalFormatVersionToWrite(5);
        confReturn.setAllowEphemeralPorts(false);
        confReturn.setBookiePort(PortManager.nextFreePort());
        confReturn.setGcWaitTime(1000);
        confReturn.setDiskUsageThreshold(0.999f);
        confReturn.setDiskUsageWarnThreshold(0.99f);
        confReturn.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);
        confReturn.setProperty(DbLedgerStorage.WRITE_CACHE_MAX_SIZE_MB, 4);
        confReturn.setProperty(DbLedgerStorage.READ_AHEAD_CACHE_MAX_SIZE_MB, 4);
        confReturn.setZkRetryBackoffMaxRetries(0);
        confReturn.setListeningInterface(getLoopbackInterfaceName());
        confReturn.setAllowLoopback(true);;
        return confReturn;
    }


    public static void setUp() throws Exception {
        rootDir = createTempDir("bkTest", ".dir");
        curDir = new File(rootDir, BookKeeperConstants.CURRENT_DIR);
        conf = getServerConfiguration();
        dirsMgr = new LedgerDirsManager(
                conf,
                new File[] { rootDir },
                new DiskChecker(
                        conf.getDiskUsageThreshold(),
                        conf.getDiskUsageWarnThreshold()));
        entryLogger = new EntryLogger(conf, dirsMgr);
        entryLogManager= (EntryLogManagerForSingleEntryLog) entryLogger.getEntryLogManager();
    }



    @After
    public void tearDown() throws Exception {
        if (null != entryLogger) {
            entryLogger.shutdown();
        }

        for (File dir : tempDirs) {
            FileUtils.deleteDirectory(dir);
        }
        tempDirs.clear();
    }


    private static ByteBuf generateEntry(long ledger, long entry) {
        byte[] data = generateDataString(ledger, entry).getBytes();
        ByteBuf bb = Unpooled.buffer(8 + 8 + data.length);
        bb.writeLong(ledger);
        bb.writeLong(entry);
        bb.writeBytes(data);
        return bb;
    }

    private static String getLoopbackInterfaceName() {
        try {
            Enumeration<NetworkInterface> nifs = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface nif : Collections.list(nifs)) {
                if (nif.isLoopback()) {
                    return nif.getName();
                }
            }
        } catch (SocketException se) {
            return null;
        }
        return null;
    }

    private static String generateDataString(long ledger, long entry) {
        return ("ledger-" + ledger + "-" + entry);
    }



}
