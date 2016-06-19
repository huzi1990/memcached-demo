package com.alipay.zixiao.protocol.binary;

import com.alipay.zixiao.cache.CacheElement;
import com.alipay.zixiao.protocol.Op;
import com.alipay.zixiao.protocol.ResponseMessage;
import com.alipay.zixiao.protocol.exceptions.UnknownCommandException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteOrder;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 二进制编码
 */
@ChannelHandler.Sharable
public class MemcachedBinaryResponseEncoder<CACHE_ELEMENT extends CacheElement> extends SimpleChannelUpstreamHandler {

    private ConcurrentHashMap<Integer, ChannelBuffer> corkedBuffers = new ConcurrentHashMap<Integer, ChannelBuffer>();

    final Logger logger = LoggerFactory.getLogger(MemcachedBinaryResponseEncoder.class);

    public static enum ResponseCode {
        OK(0x0000),
        KEYNF(0x0001),
        KEYEXISTS(0x0002),
        TOOLARGE(0x0003),
        INVARG(0x0004),
        NOT_STORED(0x0005),
        UNKNOWN(0x0081),
        OOM(0x00082);

        public short code;

        ResponseCode(int code) {
            this.code = (short)code;
        }
    }

    public ResponseCode getStatusCode(ResponseMessage command) {
        Op cmd = command.cmd.op;
        if (cmd == Op.GET || cmd == Op.GETS) {
            return ResponseCode.OK;
        } else if (cmd == Op.SET || cmd == Op.CAS) {
            switch (command.response) {
                case EXISTS:
                    return ResponseCode.KEYEXISTS;
                case NOT_FOUND:
                    return ResponseCode.KEYNF;
                case NOT_STORED:
                    return ResponseCode.NOT_STORED;
                case STORED:
                    return ResponseCode.OK;
            }
        } else if (cmd == Op.DELETE) {
            switch (command.deleteResponse) {
                case DELETED:
                    return ResponseCode.OK;
                case NOT_FOUND:
                    return ResponseCode.KEYNF;
            }
        }
        return ResponseCode.UNKNOWN;
    }

    /**
     * 构建响应头
     * @param bcmd
     * @param extrasBuffer
     * @param keyBuffer
     * @param valueBuffer
     * @param responseCode
     * @param opaqueValue
     * @param casUnique
     * @return
     */
    public ChannelBuffer constructHeader(MemcachedBinaryCommandDecoder.BinaryOp bcmd, ChannelBuffer extrasBuffer, ChannelBuffer keyBuffer, ChannelBuffer valueBuffer, short responseCode, int opaqueValue, long casUnique) {
        // 将相应信息转换成二进制
        ChannelBuffer header = ChannelBuffers.buffer(ByteOrder.BIG_ENDIAN, 24);
        header.writeByte((byte)0x81);
        header.writeByte(bcmd.code);
        short keyLength = (short) (keyBuffer != null ? keyBuffer.capacity() :0);

        header.writeShort(keyLength);
        int extrasLength = extrasBuffer != null ? extrasBuffer.capacity() : 0;
        //过期时间的长度
        header.writeByte((byte) extrasLength);
        header.writeByte((byte)0);
        header.writeShort(responseCode);

        int dataLength = valueBuffer != null ? valueBuffer.capacity() : 0;
        header.writeInt(dataLength + keyLength + extrasLength); // data length
        header.writeInt(opaqueValue); // opaque

        header.writeLong(casUnique);

        return header;
    }

    /**
     * 处理相应的异常情况
     *
     * @param ctx
     * @param e
     * @throws Exception
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        try {
            throw e.getCause();
        } catch (UnknownCommandException unknownCommand) {
            if (ctx.getChannel().isOpen())
                ctx.getChannel().write(constructHeader(MemcachedBinaryCommandDecoder.BinaryOp.Noop, null, null, null, (short)0x0081, 0, 0));
        } catch (Throwable err) {
            logger.error("error", err);
            if (ctx.getChannel().isOpen())
                ctx.getChannel().close();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void messageReceived(ChannelHandlerContext channelHandlerContext, MessageEvent messageEvent) throws Exception {
        ResponseMessage<CACHE_ELEMENT> command = (ResponseMessage<CACHE_ELEMENT>) messageEvent.getMessage();
        Object additional = messageEvent.getMessage();

        MemcachedBinaryCommandDecoder.BinaryOp bcmd = MemcachedBinaryCommandDecoder.BinaryOp.forCommandMessage(command.cmd);

        ChannelBuffer extrasBuffer = null;

        // 写入key
        ChannelBuffer keyBuffer = null;
        if (bcmd.addKeyToResponse && command.cmd.keys != null && command.cmd.keys.size() != 0) {
            keyBuffer = ChannelBuffers.wrappedBuffer(command.cmd.keys.get(0).bytes);
        }

        // 写入内容
        ChannelBuffer valueBuffer = null;
        if (command.elements != null) {
            extrasBuffer = ChannelBuffers.buffer(ByteOrder.BIG_ENDIAN, 4);
            CacheElement element = command.elements[0];
            extrasBuffer.writeShort((short) (element != null ? element.getExpire() : 0));
            extrasBuffer.writeShort((short) (element != null ? element.getFlags() : 0));

            if ((command.cmd.op == Op.GET || command.cmd.op == Op.GETS)) {
                if (element != null) {
                    valueBuffer = ChannelBuffers.wrappedBuffer(element.getData());
                } else {
                    valueBuffer = ChannelBuffers.buffer(0);
                }
            }
        }
        long casUnique = 0;
        if (command.elements != null && command.elements.length != 0 && command.elements[0] != null) {
            casUnique = command.elements[0].getCasUnique();
        }

        ChannelBuffer headerBuffer = constructHeader(bcmd, extrasBuffer, keyBuffer, valueBuffer,
                getStatusCode(command).code, command.cmd.opaque, casUnique);

        // 写入其他内容
        if (bcmd.noreply) {
            int totalCapacity =
                    headerBuffer.capacity() + (extrasBuffer != null ? extrasBuffer.capacity() : 0)
                    + (keyBuffer != null ? keyBuffer.capacity() : 0) + (valueBuffer != null ?
                            valueBuffer.capacity() :
                            0);

            ChannelBuffer corkedResponse = cork(command.cmd.opaque, totalCapacity);

            corkedResponse.writeBytes(headerBuffer);
            if (extrasBuffer != null)
                corkedResponse.writeBytes(extrasBuffer);
            if (keyBuffer != null)
                corkedResponse.writeBytes(keyBuffer);
            if (valueBuffer != null)
                corkedResponse.writeBytes(valueBuffer);
        } else {
            if (corkedBuffers.containsKey(command.cmd.opaque))
                uncork(command.cmd.opaque, messageEvent.getChannel());

            writePayload(messageEvent, extrasBuffer, keyBuffer, valueBuffer, headerBuffer);
        }

    }

    private ChannelBuffer cork(int opaque, int totalCapacity) {
        if (corkedBuffers.containsKey(opaque)) {
            ChannelBuffer corkedResponse = corkedBuffers.get(opaque);
            ChannelBuffer oldBuffer = corkedResponse;
            corkedResponse = ChannelBuffers.buffer(ByteOrder.BIG_ENDIAN, totalCapacity + corkedResponse.capacity());
            corkedResponse.writeBytes(oldBuffer);
            oldBuffer.clear();

            corkedBuffers.remove(opaque);
            corkedBuffers.put(opaque, corkedResponse);
            return corkedResponse;
        } else {
            ChannelBuffer buffer = ChannelBuffers.buffer(ByteOrder.BIG_ENDIAN, totalCapacity);
            corkedBuffers.put(opaque, buffer);
            return buffer;
        }
    }

    private void uncork(int opaque, Channel channel) {
        ChannelBuffer corkedBuffer = corkedBuffers.get(opaque);
        assert corkedBuffer !=  null;
        channel.write(corkedBuffer);
        corkedBuffers.remove(opaque);
    }

    private void writePayload(MessageEvent messageEvent, ChannelBuffer extrasBuffer, ChannelBuffer keyBuffer, ChannelBuffer valueBuffer, ChannelBuffer headerBuffer) {
        if (messageEvent.getChannel().isOpen()) {
            messageEvent.getChannel().write(headerBuffer);
            if (extrasBuffer != null)
                messageEvent.getChannel().write(extrasBuffer);
            if (keyBuffer != null)
                messageEvent.getChannel().write(keyBuffer);
            if (valueBuffer != null)
                messageEvent.getChannel().write(valueBuffer);
        }
    }
}
