/**
 *  Copyright 2008 ThimbleWare Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alipay.zixiao.protocol;

import com.alipay.zixiao.cache.Cache;
import com.alipay.zixiao.cache.CacheElement;
import com.alipay.zixiao.cache.Key;
import com.alipay.zixiao.protocol.exceptions.UnknownCommandException;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

@ChannelHandler.Sharable
public final class MemcachedCommandHandler<CACHE_ELEMENT extends CacheElement> extends SimpleChannelUpstreamHandler {

    final Logger logger = LoggerFactory.getLogger(MemcachedCommandHandler.class);

    public final AtomicInteger curr_conns = new AtomicInteger();
    public final AtomicInteger total_conns = new AtomicInteger();



    public final int idle_limit;
    public final boolean verbose;



    /**
     * The actual physical data storage.
     */
    private final Cache<CACHE_ELEMENT> cache;

    /**
     * The channel group for the entire daemon, used for handling global cleanup on shutdown.
     */
    private final DefaultChannelGroup channelGroup;

    /**
     * Construct the server session handler
     *  @param cache            the cache to use
     * @param verbosity        verbosity level for debugging
     * @param idle             how long sessions can be idle for
     * @param channelGroup
     */
    public MemcachedCommandHandler(Cache cache, boolean verbosity, int idle, DefaultChannelGroup channelGroup) {
        this.cache = cache;

        verbose = verbosity;
        idle_limit = idle;
        this.channelGroup = channelGroup;
    }


    /**
     * On open we manage some statistics, and add this connection to the channel group.
     *
     * @param channelHandlerContext
     * @param channelStateEvent
     * @throws Exception
     */
    @Override
    public void channelOpen(ChannelHandlerContext channelHandlerContext, ChannelStateEvent channelStateEvent) throws Exception {
        total_conns.incrementAndGet();
        curr_conns.incrementAndGet();
        channelGroup.add(channelHandlerContext.getChannel());
    }

    /**
     * On close we manage some statistics, and remove this connection from the channel group.
     *
     * @param channelHandlerContext
     * @param channelStateEvent
     * @throws Exception
     */
    @Override
    public void channelClosed(ChannelHandlerContext channelHandlerContext, ChannelStateEvent channelStateEvent) throws Exception {
        curr_conns.decrementAndGet();
        channelGroup.remove(channelHandlerContext.getChannel());
    }


    /**
     * The actual meat of the matter.  Turn CommandMessages into executions against the physical cache, and then
     * pass on the downstream messages.
     *
     * @param channelHandlerContext
     * @param messageEvent
     * @throws Exception
     */

    @Override
    @SuppressWarnings("unchecked")
    public void messageReceived(ChannelHandlerContext channelHandlerContext, MessageEvent messageEvent) throws Exception {
        if (!(messageEvent.getMessage() instanceof CommandMessage)) {
            // Ignore what this encoder can't encode.
            channelHandlerContext.sendUpstream(messageEvent);
            return;
        }

        CommandMessage<CACHE_ELEMENT> command = (CommandMessage<CACHE_ELEMENT>) messageEvent.getMessage();
        Op cmd = command.op;
        int cmdKeysSize = command.keys == null ? 0 : command.keys.size();

        // first process any messages in the delete queue
        cache.asyncEventPing();

        // now do the real work
        if (this.verbose) {
            StringBuilder log = new StringBuilder();
            log.append(cmd);
            if (command.element != null) {
                log.append(" ").append(command.element.getKey());
            }
            for (int i = 0; i < cmdKeysSize; i++) {
                log.append(" ").append(command.keys.get(i));
            }
            logger.info(log.toString());
        }

        Channel channel = messageEvent.getChannel();
        if (cmd == null) handleNoOp(channelHandlerContext, command);
        else
        switch (cmd) {
            case GET:
            case GETS:
                handleGets(channelHandlerContext, command, channel);
                break;
            case DELETE:
                handleDelete(channelHandlerContext, command, channel);
                break;
            case SET:
                handleSet(channelHandlerContext, command, channel);
                break;
            case CAS:
                handleCas(channelHandlerContext, command, channel);
                break;
            default:
                 throw new UnknownCommandException("unknown command");
        }
    }

    protected void handleNoOp(ChannelHandlerContext channelHandlerContext, CommandMessage<CACHE_ELEMENT> command) {
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command));
    }


    protected void handleDelete(ChannelHandlerContext channelHandlerContext, CommandMessage<CACHE_ELEMENT> command, Channel channel) {
        Cache.DeleteResponse dr = cache.delete(command.keys.get(0), command.time);
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withDeleteResponse(dr), channel.getRemoteAddress());
    }

    

    protected void handleCas(ChannelHandlerContext channelHandlerContext, CommandMessage<CACHE_ELEMENT> command, Channel channel) {
        Cache.StoreResponse ret;
        ret = cache.cas(command.cas_key, command.element);
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
    }

    protected void handleSet(ChannelHandlerContext channelHandlerContext, CommandMessage<CACHE_ELEMENT> command, Channel channel) {
        Cache.StoreResponse ret;
        ret = cache.set(command.element);
        Channels.fireMessageReceived(channelHandlerContext, new ResponseMessage(command).withResponse(ret), channel.getRemoteAddress());
    }

    protected void handleGets(ChannelHandlerContext channelHandlerContext, CommandMessage<CACHE_ELEMENT> command, Channel channel) {
        Key[] keys = new Key[command.keys.size()];
        keys = command.keys.toArray(keys);
        CACHE_ELEMENT[] results = get(keys);
        ResponseMessage<CACHE_ELEMENT> resp = new ResponseMessage<CACHE_ELEMENT>(command).withElements(results);
        Channels.fireMessageReceived(channelHandlerContext, resp, channel.getRemoteAddress());
    }

    /**
     * Get an element from the cache
     *
     * @param keys the key for the element to lookup
     * @return the element, or 'null' in case of cache miss.
     */
    private CACHE_ELEMENT[] get(Key... keys) {
        return cache.get(keys);
    }


    /**
     * @return the current time in seconds (from epoch), used for expiries, etc.
     */
    private static int Now() {
        return (int) (System.currentTimeMillis() / 1000);
    }




}