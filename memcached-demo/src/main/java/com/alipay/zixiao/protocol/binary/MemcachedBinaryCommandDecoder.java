package com.alipay.zixiao.protocol.binary;

import com.alipay.zixiao.cache.CacheElement;
import com.alipay.zixiao.cache.Key;
import com.alipay.zixiao.cache.LocalCacheElement;
import com.alipay.zixiao.protocol.CommandMessage;
import com.alipay.zixiao.protocol.Op;
import com.alipay.zixiao.protocol.exceptions.MalformedCommandException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.ArrayList;

/**
 * 二进制解码器
 */
@ChannelHandler.Sharable
public class MemcachedBinaryCommandDecoder extends FrameDecoder {

    public static final Charset USASCII = Charset.forName("US-ASCII");

    /**
     * 二进制对应枚举
     */
    public static enum BinaryOp {
        Get(0x00, Op.GET, false),
        Set(0x01, Op.SET, false),
        Delete(0x04, Op.DELETE, false),
        GetQ(0x09, Op.GET, false),
        Noop(0x0A, null, false),
        GetK(0x0C, Op.GET, false, true),
        GetKQ(0x0D, Op.GET, true, true),
        SetQ(0x11, Op.SET, true),
        DeleteQ(0x14, Op.DELETE, true);


        public byte code;
        public Op correspondingOp;
        public boolean noreply;
        public boolean addKeyToResponse = false;

        BinaryOp(int code, Op correspondingOp, boolean noreply) {
            this.code = (byte)code;
            this.correspondingOp = correspondingOp;
            this.noreply = noreply;
        }

        BinaryOp(int code, Op correspondingOp, boolean noreply, boolean addKeyToResponse) {
            this.code = (byte)code;
            this.correspondingOp = correspondingOp;
            this.noreply = noreply;
            this.addKeyToResponse = addKeyToResponse;
        }

        public static BinaryOp forCommandMessage(CommandMessage msg) {
            for (BinaryOp binaryOp : values()) {
                if (binaryOp.correspondingOp == msg.op && binaryOp.noreply == msg.noreply && binaryOp.addKeyToResponse == msg.addKeyToResponse) {
                    return binaryOp;
                }
            }

            return null;
        }

    }

    protected Object decode(ChannelHandlerContext channelHandlerContext, Channel channel, ChannelBuffer channelBuffer) throws Exception {

        // 协议头必须满足不小于24
        if (channelBuffer.readableBytes() < 24) return null;

        // get the header
        channelBuffer.markReaderIndex();
        ChannelBuffer headerBuffer = ChannelBuffers.buffer(ByteOrder.BIG_ENDIAN, 24);
        channelBuffer.readBytes(headerBuffer);

        short magic = headerBuffer.readUnsignedByte();

        // magic 值应该为 0x80
        if (magic != 0x80) {
            headerBuffer.resetReaderIndex();

            throw new MalformedCommandException("binary request payload is invalid, magic byte incorrect");
        }

        //解析二进制协议头
        short opcode = headerBuffer.readUnsignedByte();
        short keyLength = headerBuffer.readShort();
        short extraLength = headerBuffer.readUnsignedByte();
        short dataType = headerBuffer.readUnsignedByte();
        short reserved = headerBuffer.readShort();
        int totalBodyLength = headerBuffer.readInt();
        int opaque = headerBuffer.readInt();
        long cas = headerBuffer.readLong();

        // 读取内容
        if (channelBuffer.readableBytes() < totalBodyLength) {
            channelBuffer.resetReaderIndex();
            return null;
        }

        // 将命令请求转换成handler处理的模型
        BinaryOp bcmd = BinaryOp.values()[opcode];

        Op cmdType = bcmd.correspondingOp;
        CommandMessage cmdMessage = CommandMessage.command(cmdType);
        cmdMessage.noreply = bcmd.noreply;
        cmdMessage.cas_key = cas;
        cmdMessage.opaque = opaque;
        cmdMessage.addKeyToResponse = bcmd.addKeyToResponse;

        // 获得额外的内容
        ChannelBuffer extrasBuffer = ChannelBuffers.buffer(ByteOrder.BIG_ENDIAN, extraLength);
        channelBuffer.readBytes(extrasBuffer);

        // 获得key模型
        if (keyLength != 0) {
            ChannelBuffer keyBuffer = ChannelBuffers.buffer(ByteOrder.BIG_ENDIAN, keyLength);
            channelBuffer.readBytes(keyBuffer);

            ArrayList<Key> keys = new ArrayList<Key>();
            keys.add(new Key(keyBuffer.copy()));

            cmdMessage.keys = keys;

            if (cmdType == Op.SET)

            {
                //过期时间
                long expire = ((short) (extrasBuffer.capacity() != 0 ?
                        extrasBuffer.readUnsignedShort() :
                        0)) * 1000;
                //标记
                short flags = (short) (extrasBuffer.capacity() != 0 ?
                        extrasBuffer.readUnsignedShort() :
                        0);

                // 内容
                int size = totalBodyLength - keyLength - extraLength;

                cmdMessage.element = new LocalCacheElement(new Key(keyBuffer.slice()), flags,
                        expire != 0 && expire < CacheElement.THIRTY_DAYS ?
                                LocalCacheElement.Now() + expire :
                                expire, 0L);
                ChannelBuffer data = ChannelBuffers.buffer(size);
                channelBuffer.readBytes(data);
                cmdMessage.element.setData(data);
            }
        }

        return cmdMessage;
    }
}
