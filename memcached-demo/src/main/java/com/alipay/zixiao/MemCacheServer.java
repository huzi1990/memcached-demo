package com.alipay.zixiao;

import com.alipay.zixiao.cache.Cache;
import com.alipay.zixiao.cache.CacheElement;
import com.alipay.zixiao.protocol.binary.MemcachedBinaryPipelineFactory;
import com.alipay.zixiao.protocol.text.MemcachedPipelineFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;


public class MemCacheServer <CACHE_ELEMENT extends CacheElement>
{

    final Logger log = LoggerFactory.getLogger(MemCacheServer.class);

    public static String memcachedVersion = "0.9";

    private int frameSize = 32768 * 1024;

    private boolean binary = false;
    private boolean              verbose;
    private int                  idleTime;
    private InetSocketAddress    addr;
    private Cache<CACHE_ELEMENT> cache;

    private boolean running = false;
    private ServerSocketChannelFactory channelFactory;
    private DefaultChannelGroup        allChannels;


    public MemCacheServer() {
    }

    public MemCacheServer(Cache<CACHE_ELEMENT> cache) {
        this.cache = cache;
    }

    /**
     * Bind the network connection and start the network processing threads.
     */
    public void start() {
        // TODO provide tweakable options here for passing in custom executors.
        channelFactory =
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool());

        allChannels = new DefaultChannelGroup("jmemcachedChannelGroup");

        ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);

        ChannelPipelineFactory pipelineFactory;
        if (binary)
            pipelineFactory = createMemcachedBinaryPipelineFactory(cache, memcachedVersion, verbose, idleTime, allChannels);
        else
            pipelineFactory = createMemcachedPipelineFactory(cache, memcachedVersion, verbose, idleTime, frameSize, allChannels);

        bootstrap.setPipelineFactory(pipelineFactory);
        bootstrap.setOption("sendBufferSize", 65536 );
        bootstrap.setOption("receiveBufferSize", 65536);

        Channel serverChannel = bootstrap.bind(addr);
        allChannels.add(serverChannel);

        log.info("Listening on " + String.valueOf(addr.getHostName()) + ":" + addr.getPort());

        running = true;
    }

    protected ChannelPipelineFactory createMemcachedBinaryPipelineFactory(
            Cache cache, String memcachedVersion, boolean verbose, int idleTime, DefaultChannelGroup allChannels) {
        return new MemcachedBinaryPipelineFactory(cache, memcachedVersion, verbose, idleTime, allChannels);
    }

    protected ChannelPipelineFactory createMemcachedPipelineFactory(
            Cache cache, String memcachedVersion, boolean verbose, int idleTime, int receiveBufferSize, DefaultChannelGroup allChannels) {
        return new MemcachedPipelineFactory(cache, memcachedVersion, verbose, idleTime, receiveBufferSize, allChannels);
    }

    public void stop() {
        log.info("terminating daemon; closing all channels");

        ChannelGroupFuture future = allChannels.close();
        future.awaitUninterruptibly();
        if (!future.isCompleteSuccess()) {
            throw new RuntimeException("failure to complete closing all network channels");
        }
        log.info("channels closed, freeing cache storage");
        try {
            cache.close();
        } catch (IOException e) {
            throw new RuntimeException("exception while closing storage", e);
        }
        channelFactory.releaseExternalResources();

        running = false;
        log.info("successfully shut down");
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public void setIdleTime(int idleTime) {
        this.idleTime = idleTime;
    }

    public void setAddr(InetSocketAddress addr) {
        this.addr = addr;
    }

    public Cache<CACHE_ELEMENT> getCache() {
        return cache;
    }

    public void setCache(Cache<CACHE_ELEMENT> cache) {
        this.cache = cache;
    }

    public boolean isRunning() {
        return running;
    }

    public boolean isBinary() {
        return binary;
    }

    public void setBinary(boolean binary) {
        this.binary = binary;
    }

}
