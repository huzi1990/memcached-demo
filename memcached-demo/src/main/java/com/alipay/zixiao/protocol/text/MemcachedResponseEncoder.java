package com.alipay.zixiao.protocol.text;

import com.alipay.zixiao.cache.Cache;
import com.alipay.zixiao.cache.CacheElement;
import com.alipay.zixiao.protocol.Op;
import com.alipay.zixiao.protocol.ResponseMessage;
import com.alipay.zixiao.protocol.exceptions.ClientException;
import com.alipay.zixiao.util.BufferUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alipay.zixiao.protocol.text.MemcachedPipelineFactory.USASCII;

/**
 * Response encoder for the memcached text protocol. Produces strings destined for the StringEncoder
 */
public final class MemcachedResponseEncoder<CACHE_ELEMENT extends CacheElement> extends SimpleChannelUpstreamHandler {

    final Logger logger = LoggerFactory.getLogger(MemcachedResponseEncoder.class);


    public static final ChannelBuffer CRLF = ChannelBuffers.copiedBuffer("\r\n", USASCII);
    private static final ChannelBuffer SPACE = ChannelBuffers.copiedBuffer(" ", USASCII);
    private static final ChannelBuffer VALUE = ChannelBuffers.copiedBuffer("VALUE ", USASCII);
    private static final ChannelBuffer EXISTS = ChannelBuffers.copiedBuffer("EXISTS\r\n", USASCII);
    private static final ChannelBuffer NOT_FOUND = ChannelBuffers.copiedBuffer("NOT_FOUND\r\n", USASCII);
    private static final ChannelBuffer NOT_STORED = ChannelBuffers.copiedBuffer("NOT_STORED\r\n", USASCII);
    private static final ChannelBuffer STORED = ChannelBuffers.copiedBuffer("STORED\r\n", USASCII);
    private static final ChannelBuffer DELETED = ChannelBuffers.copiedBuffer("DELETED\r\n", USASCII);
    private static final ChannelBuffer END = ChannelBuffers.copiedBuffer("END\r\n", USASCII);
    private static final ChannelBuffer OK = ChannelBuffers.copiedBuffer("OK\r\n", USASCII);
    private static final ChannelBuffer ERROR = ChannelBuffers.copiedBuffer("ERROR\r\n", USASCII);
    private static final ChannelBuffer CLIENT_ERROR = ChannelBuffers.copiedBuffer("CLIENT_ERROR\r\n", USASCII);

    /**
     * Handle exceptions in protocol processing. Exceptions are either client or internal errors.  Report accordingly.
     *
     * @param ctx
     * @param e
     * @throws Exception
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        try {
            throw e.getCause();
        } catch (ClientException ce) {
            if (ctx.getChannel().isOpen())
                ctx.getChannel().write(CLIENT_ERROR);
        } catch (Throwable tr) {
            logger.error("error", tr);
            if (ctx.getChannel().isOpen())
                ctx.getChannel().write(ERROR);
        }
    }



    @Override
    public void messageReceived(ChannelHandlerContext channelHandlerContext, MessageEvent messageEvent) throws Exception {
        ResponseMessage<CACHE_ELEMENT> command = (ResponseMessage<CACHE_ELEMENT>) messageEvent.getMessage();

        Op cmd = command.cmd.op;

        Channel channel = messageEvent.getChannel();

        switch (cmd) {
            case GET:
            case GETS:
                CacheElement[] results = command.elements;

                ChannelBuffer[] buffers = new ChannelBuffer[results.length * (9 + (cmd == Op.GETS ? 2 : 0)) + 1];
                int i = 0;
                for (CacheElement result : results) {
                    if (result != null) {
                        buffers[i++] = VALUE;
                        buffers[i++] = result.getKey().bytes;
                        buffers[i++] = SPACE;
                        buffers[i++] = BufferUtils.itoa(result.getFlags());
                        buffers[i++] = SPACE;
                        buffers[i++] = BufferUtils.itoa(result.size());
                        if (cmd == Op.GETS) {
                            buffers[i++] = SPACE;
                            buffers[i++] = BufferUtils.ltoa(result.getCasUnique());
                        }
                        buffers[i++] = CRLF;
                        buffers[i++] = result.getData();
                        buffers[i++] = CRLF;
                    }
                }
                buffers[i] = END;

                Channels.write(channel, ChannelBuffers.wrappedBuffer(buffers));
                break;
            case SET:
            case CAS:
                if (!command.cmd.noreply)
                    Channels.write(channel, storeResponse(command.response));
                break;
            case DELETE:
                if (!command.cmd.noreply)
                    Channels.write(channel, deleteResponseString(command.deleteResponse));

                break;

            default:
                Channels.write(channel, ERROR.duplicate());
                logger.error("error; unrecognized command: " + cmd);

        }

    }

    private ChannelBuffer deleteResponseString(Cache.DeleteResponse deleteResponse) {
        if (deleteResponse == Cache.DeleteResponse.DELETED) return DELETED.duplicate();
        else return NOT_FOUND.duplicate();
    }


    /**
     * Find the string response message which is equivalent to a response to a set/add/replace message
     * in the cache
     *
     * @param storeResponse the response code
     * @return the string to output on the network
     */
    private ChannelBuffer storeResponse(Cache.StoreResponse storeResponse) {
        switch (storeResponse) {
            case EXISTS:
                return EXISTS.duplicate();
            case NOT_FOUND:
                return NOT_FOUND.duplicate();
            case NOT_STORED:
                return NOT_STORED.duplicate();
            case STORED:
                return STORED.duplicate();
        }
        throw new RuntimeException("unknown store response from cache: " + storeResponse);
    }
}
