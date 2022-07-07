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
import io.netty.buffer.PooledByteBufAllocator;
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
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.*;

import static org.apache.bookkeeper.bookie.EntryLogger.UNASSIGNED_LEDGERID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

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
    public static class WriteLogTest {
        private long ledgeridToSearch;
        private List<Long> listledgerid;
        private boolean expected;
        private boolean expectedExceptionNull;
        private boolean expectedExceptionIndexOut;
        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    /*Entries to write in log file,    ledger id to search*/
                    { Arrays.asList(1L,2L,3L,4L),       1L},
                    { Arrays.asList(1L,2L,3L,4L),       2L},
                    { Arrays.asList(1L,2L,3L,4L),       4L},
                    { Arrays.asList(1L,2L,3L,4L),       5L},
                    { Arrays.asList(1L,null,3L,4L),     1L},
                    { Arrays.asList(null,2L,3L,4L),     2L},
                    { Arrays.asList(1L,2L,3L,null),     4L},
                    { Arrays.asList(1L,2L,null,4L),     5L},
                    { new ArrayList<>(),                1L}
            });
        }

        public WriteLogTest(List<Long> listledgerid, long ledgeridToSearch) throws Exception {
            this.listledgerid=listledgerid;
            this.ledgeridToSearch=ledgeridToSearch;
            getOracleTestWriteLog();
            setUp();
        }

        @Test
        public void testWriteLog() throws Exception {
            ByteBuf entry;

            try{
                for(long i  : this.listledgerid){
                    entry = generateEntry(i, 1);
                    entryLogManager.addEntry(i, entry,true);
                }

                entryLogManager.createNewLog(UNASSIGNED_LEDGERID);
                EntryLogMetadata meta = entryLogger.extractEntryLogMetadataFromIndex(0L);
                assertEquals(this.expected,meta.getLedgersMap().containsKey(ledgeridToSearch));

            }catch(NullPointerException e){
                assertTrue(expectedExceptionNull);
            }
            catch(IndexOutOfBoundsException e){
                assertTrue(expectedExceptionIndexOut);
            }


        }
        public void getOracleTestWriteLog(){
            this.expected= listledgerid.contains(ledgeridToSearch);
            if(this.listledgerid.size()==0)
                this.expectedExceptionIndexOut=  true;

            if(listledgerid.contains(null))
                this.expectedExceptionNull=true;


        }
    }

    @RunWith(Parameterized.class)
    public static class WriteMultipleLogTest {
        private long ledgeridToSearch;
        private List<Long> listledgerid;
        private long positionLog;
        private boolean expected;
        private boolean expectedExceptionNull;
        private boolean expectedExceptionFileNF;

        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][] {
                    /*Entries to write in log file,    ledger id to search,        file where to search ledger id */
                    { Arrays.asList(1L,2L,3L,4L),       1L,                        0L},
                    { Arrays.asList(1L,2L,3L,4L),       2L,                        1L},
                    { Arrays.asList(1L,2L,3L,4L),       4L,                        3L},
                    { Arrays.asList(1L,2L,3L,4L),       5L,                        4L},
                    { Arrays.asList(1L,2L,null,4L),     1L,                        0L},
                    { Arrays.asList(1L,null,3L,4L),     2L,                        1L},
                    { Arrays.asList(1L,2L,3L,null),     4L,                        3L},
                    { Arrays.asList(null,2L,3L,4L),     5L,                        4L},
                    { new ArrayList<>(),                5L,                        4L},
                    { Arrays.asList(1L,2L,3L,4L),       1L,                        2L},
                    { Arrays.asList(1L,2L,3L,4L),       2L,                        3L},
                    { Arrays.asList(1L,2L,3L,4L),       4L,                        4L},
                    { Arrays.asList(1L,2L,3L,4L),       5L,                        1L},
                    { Arrays.asList(1L,2L,null,4L),     1L,                        2L},
                    { Arrays.asList(1L,null,3L,4L),     2L,                        3L},
                    { Arrays.asList(1L,2L,3L,null),     4L,                        5L},
                    { Arrays.asList(null,2L,3L,4L),     5L,                        6L},
            });
        }

        public WriteMultipleLogTest(List<Long> listledgerid, long ledgeridToSearch,long positionLog) throws Exception {
            this.listledgerid=listledgerid;
            this.ledgeridToSearch=ledgeridToSearch;
            this.positionLog=positionLog;
            getOracleTestWriteMultipleLog();
            setUp();
        }

        @Test
        public void testWriteLog() throws Exception {
            ByteBuf entry;

            try{
                for(long i  : this.listledgerid){
                    entry = generateEntry(i, 1);
                    entryLogManager.addEntry(i, entry,true);
                    entryLogManager.createNewLog(UNASSIGNED_LEDGERID);
                }

                entryLogManager.createNewLog(UNASSIGNED_LEDGERID);
                EntryLogMetadata meta = entryLogger.extractEntryLogMetadataFromIndex(this.positionLog);
                assertEquals(this.expected,meta.getLedgersMap().containsKey(ledgeridToSearch));

            }catch(NullPointerException e){
                assertTrue(expectedExceptionNull);
            }
            catch(FileNotFoundException e){
                assertTrue(expectedExceptionFileNF);
            }

        }
        public void getOracleTestWriteMultipleLog(){
            this.expected = listledgerid.contains(ledgeridToSearch) && (positionLog==ledgeridToSearch-1);
            if(this.listledgerid.size()==0)
                this.expectedExceptionFileNF=  true;

            if(listledgerid.contains(null))
                this.expectedExceptionNull=true;
        }
    }


    @RunWith(Parameterized.class)
    public static class WhiteBoxTest1  {

        private boolean isActiveLogChannelNull;
        private long defaulLogId=0L;
        private boolean rollLong;
        private long ledgerid=1L;
        private int ensize;
        private long expected;



        @Parameterized.Parameters
        public static Collection<Object[]> data() {
            return Arrays.asList(new Object[][] {
                    /*logChannel is null,      rollLong,     size   */
                    { true,                    true,           1,       },
                    { true,                    false,          1,       },
                    { false,                   true,           1,       },
                    { false,                   false,          1,       },
                    { false,                   true,           Integer.MAX_VALUE},
                    { true,                    true,           Integer.MAX_VALUE},
                    { true,                    false,          Integer.MAX_VALUE},
                    { false,                   false,          Integer.MAX_VALUE},
                    { false,                   false,          -1       },
                    { true,                    false,          -1,       },
                    { false,                   true,           -1,       },
                    { false,                   false,          -1,       },

            });
        }

        public WhiteBoxTest1(boolean isActiveLogChannelNull,boolean rollLong,int ensize) throws Exception {
            this.isActiveLogChannelNull = isActiveLogChannelNull;
            this.rollLong=rollLong;
            this.ensize=ensize;
            setUp();
            getOracle();
        }

        @Test
        public void testWhiteBox() throws Exception {

            EntryLogger.BufferedLogChannel buffer;

            if(isActiveLogChannelNull){
                entryLogManager.createNewLog(UNASSIGNED_LEDGERID, "Test");
            }

            buffer = entryLogManager.getCurrentLogForLedgerForAddEntry(ledgerid,ensize,rollLong);
            assertEquals(expected,buffer.getLogId());

        }

        public void getOracle(){

            expected=defaulLogId;

            if(ensize==Integer.MAX_VALUE)
                expected=1;
        }


    }


    @RunWith(MockitoJUnitRunner.class)
    public static class WhiteBoxTest2 {

        @Mock
        EntryLogger.BufferedLogChannel channel;

        @InjectMocks
        static EntryLogManagerForSingleEntryLog entryLogManagerInject;

        static {
            try {
                entryLogManagerInject = injectMock();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private static EntryLogManagerForSingleEntryLog entryLogManagerNoInject;

        static {
            try {
                entryLogManagerNoInject = getNoMockManager();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private static EntryLogManagerForSingleEntryLog entryLogManagerTest;

        @Test(expected = IOException.class)
        public void testWhiteBox1() throws Exception {
            setUpWhiteBoxMethod(true,true);
            entryLogManagerTest.flushRotatedLogs();
        }

        @Test(expected = IOException.class)
        public void testWhiteBox2() throws Exception {
            setUpWhiteBoxMethod(false,true);
            entryLogManagerTest.flushRotatedLogs();
            assertEquals(0,entryLogManagerTest.rotatedLogChannels.size());
        }
        @Test(expected = IOException.class)
        public void testWhiteBox3() throws Exception {
            setUpWhiteBoxMethod(false,true);
            entryLogManagerTest.flushRotatedLogs();
            assertEquals(0,entryLogManagerTest.rotatedLogChannels.size());
        }

        @Test
        public void testWhiteBox4() throws Exception {
            setUpWhiteBoxMethod(false,false);
            entryLogManagerTest.flushRotatedLogs();
            assertEquals(0,entryLogManagerTest.rotatedLogChannels.size());
        }

        @Test
        public void testPit6() throws Exception {
            setUpWhiteBoxMock();
            List<File> files = new ArrayList<>();

            File file1 = File.createTempFile("temp1", null);
            File file2 = File.createTempFile("temp2", null);
            File file3 = File.createTempFile("temp3", null);
            File file4 = File.createTempFile("temp4", null);

            files.add(file1);
            files.add(file2);
            files.add(file3);
            files.add(file4);

            List<File> spyList = Mockito.spy(files);
            Mockito.doReturn(file1).when(spyList).get(anyInt());

            entryLogManagerTest.getDirForNextEntryLog(spyList);
            Mockito.verify(spyList).size();
            
        }

        @Test
        public void testPit7() throws Exception {
            setUpWhiteBoxMock();
            entryLogManagerTest.checkpoint();

            assertEquals(0, entryLogManagerTest.getRotatedLogChannels().size());
        }

        public void setUpWhiteBoxMethod(boolean hasRotatedLogChannels,boolean throwException) throws Exception {

            if(hasRotatedLogChannels){
                EntryLogManagerForSingleEntryLog entryLogManagerHelper = (EntryLogManagerForSingleEntryLog) entryLogger.getEntryLogManager();
                entryLogManagerHelper.createNewLog(UNASSIGNED_LEDGERID, "Test 1");
                entryLogManagerHelper.createNewLog(UNASSIGNED_LEDGERID, "Test 2");
            }

            if(throwException){
                entryLogManagerTest=entryLogManagerInject;
                doThrow(new IOException()).when(channel).flushAndForceWrite(isA(boolean.class));
                entryLogManagerTest.setCurrentLogForLedgerAndAddToRotate(1,channel);
                entryLogManagerTest.setCurrentLogForLedgerAndAddToRotate(2,channel);

            }
            else{
                entryLogManagerTest=entryLogManagerNoInject;
            }

        }

        public void setUpWhiteBoxMock() throws IOException {
            entryLogManagerTest=getNoMockManager();
            channel=mock(EntryLogger.BufferedLogChannel.class);
            doNothing().when(channel).flushAndForceWrite(anyBoolean());
            entryLogManagerTest.setCurrentLogForLedgerAndAddToRotate(0,channel);
            entryLogManagerTest.setCurrentLogForLedgerAndAddToRotate(1,channel);

        }

        public  static  EntryLogManagerForSingleEntryLog injectMock() throws IOException {
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

            entryLoggerAllocator = new EntryLoggerAllocator(conf, dirsMgr, recentlyCreatedEntryLogsStatus,
                    0, PooledByteBufAllocator.DEFAULT);

            EntryLogManagerForSingleEntryLog entryLogManager = new EntryLogManagerForSingleEntryLog(conf, dirsMgr,
                    entryLoggerAllocator, listeners, recentlyCreatedEntryLogsStatus);

            return entryLogManager;
        }

        public  static  EntryLogManagerForSingleEntryLog getNoMockManager() throws IOException {
            rootDir = createTempDir("bkTest", ".dir");
            curDir = new File(rootDir, BookKeeperConstants.CURRENT_DIR);
            conf = getServerConfiguration();
            dirsMgr = new LedgerDirsManager(
                    conf,
                    new File[] { rootDir },
                    new DiskChecker(
                            conf.getDiskUsageThreshold(),
                            conf.getDiskUsageWarnThreshold()));
            EntryLogger entryLogger = new EntryLogger(conf, dirsMgr);

            return (EntryLogManagerForSingleEntryLog) entryLogger.getEntryLogManager();
        }

    }

    public static void setUpWhiteBox() throws Exception {
        setUp();

        recentlyCreatedEntryLogsStatus= mock(EntryLogger.RecentEntryLogsStatus.class);
        doNothing().when(recentlyCreatedEntryLogsStatus).createdEntryLog(isA(Long.class));

        entryLoggerAllocator = new EntryLoggerAllocator(conf, dirsMgr, recentlyCreatedEntryLogsStatus,
                0, PooledByteBufAllocator.DEFAULT);

        entryLogManager = new EntryLogManagerForSingleEntryLog(conf, dirsMgr,
                entryLoggerAllocator, listeners, recentlyCreatedEntryLogsStatus);

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
