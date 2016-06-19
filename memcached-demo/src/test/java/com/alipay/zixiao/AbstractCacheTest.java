package com.alipay.zixiao;

import com.alipay.zixiao.cache.Cache;
import com.alipay.zixiao.cache.CacheImpl;
import com.alipay.zixiao.cache.Key;
import com.alipay.zixiao.cache.LocalCacheElement;
import com.alipay.zixiao.cache.hash.ConcurrentLinkedHashMap;
import com.alipay.zixiao.cache.storage.CacheStorage;
import com.alipay.zixiao.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;

/**
 */
public abstract class AbstractCacheTest {


    protected static final int MAX_BYTES = (int) Bytes.valueOf("4m").bytes();
    public static final int MAX_SIZE = 1000;
    protected       MemCacheServer<LocalCacheElement> daemon;
    private         int                               port;
    protected       Cache<LocalCacheElement>          cache;
    private final   ProtocolMode                      protocolMode;

    public AbstractCacheTest( ProtocolMode protocolMode) {
        this.protocolMode = protocolMode;
    }


    public static enum ProtocolMode {
        TEXT, BINARY
    }

    @Parameterized.Parameters
    public static Collection blockSizeValues() {
        return Arrays.asList(new Object[][] {
                {  ProtocolMode.TEXT },
//                {  ProtocolMode.BINARY }

        });
    }

    @Before
    public void setup() throws IOException {
        // 创建守护线程
        daemon = new MemCacheServer<LocalCacheElement>();
        CacheStorage<Key, LocalCacheElement> cacheStorage = ConcurrentLinkedHashMap
                .create(ConcurrentLinkedHashMap.EvictionPolicy.FIFO, MAX_SIZE, MAX_BYTES);

        daemon.setCache(new CacheImpl(cacheStorage));
        daemon.setBinary(protocolMode == ProtocolMode.BINARY);
        
        port = AvailablePortFinder.getNextAvailable();
        daemon.setAddr(new InetSocketAddress(port));
        daemon.setVerbose(false);
        daemon.start();

        cache = daemon.getCache();
    }


    @After
    public void teardown() throws Exception{
        if (daemon.isRunning())
            daemon.stop();
    }





    public ProtocolMode getProtocolMode() {
        return protocolMode;
    }

    public int getPort() {
        return port;
    }
}
