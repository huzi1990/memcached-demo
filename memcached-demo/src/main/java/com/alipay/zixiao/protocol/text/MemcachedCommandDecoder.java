package com.alipay.zixiao.protocol.text;

import com.alipay.zixiao.cache.CacheElement;
import com.alipay.zixiao.cache.Key;
import com.alipay.zixiao.cache.LocalCacheElement;
import com.alipay.zixiao.protocol.CommandMessage;
import com.alipay.zixiao.protocol.Op;
import com.alipay.zixiao.protocol.SessionStatus;
import com.alipay.zixiao.protocol.exceptions.IncorrectlyTerminatedPayloadException;
import com.alipay.zixiao.protocol.exceptions.InvalidProtocolStateException;
import com.alipay.zixiao.protocol.exceptions.MalformedCommandException;
import com.alipay.zixiao.protocol.exceptions.UnknownCommandException;
import com.alipay.zixiao.util.BufferUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferIndexFinder;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import java.util.ArrayList;
import java.util.List;

/**
 * The MemcachedCommandDecoder is responsible for taking lines from the MemcachedFrameDecoder and parsing them
 * into CommandMessage instances for handling by the MemcachedCommandHandler
 * <p/>
 * Protocol status is held in the SessionStatus instance which is shared between each of the decoders in the pipeline.
 */
public final class MemcachedCommandDecoder extends FrameDecoder {

    private static final int MIN_BYTES_LINE = 2;
    private SessionStatus status;

    private static final ChannelBuffer NOREPLY = ChannelBuffers.wrappedBuffer("noreply".getBytes());


    public MemcachedCommandDecoder(SessionStatus status) {
        this.status = status;
    }

    /**
     * Index finder which locates a byte which is neither a {@code CR ('\r')}
     * nor a {@code LF ('\n')}.
     */
    static ChannelBufferIndexFinder CRLF_OR_WS = new ChannelBufferIndexFinder() {
        public final boolean find(ChannelBuffer buffer, int guessedIndex) {
            byte b = buffer.getByte(guessedIndex);
            return b == ' ' || b == '\r' || b == '\n';
        }
    };

    static boolean eol(int pos, ChannelBuffer buffer) {
        return buffer.readableBytes() >= MIN_BYTES_LINE && buffer.getByte(buffer.readerIndex() + pos) == '\r' && buffer.getByte(buffer.readerIndex() + pos+1) == '\n';
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
        if (status.state == SessionStatus.State.READY) {
            ChannelBuffer in = buffer.slice();

            // 将命令解析
            List<ChannelBuffer> pieces = new ArrayList<ChannelBuffer>(6);
            if (in.readableBytes() < MIN_BYTES_LINE) return null;
            int pos = in.bytesBefore(CRLF_OR_WS);
            boolean eol = false;
            do {
                if (pos != -1) {
                    eol = eol(pos, in);
                    int skip = eol ? MIN_BYTES_LINE : 1;
                    ChannelBuffer slice = in.readSlice(pos);
                    slice.readerIndex(0);
                    pieces.add(slice);
                    in.skipBytes(skip);
                    if (eol) break;
                }
            } while ((pos = in.bytesBefore(CRLF_OR_WS)) != -1);
            if (eol) {
                buffer.skipBytes(in.readerIndex());

                return processLine(pieces, channel, ctx);
            }
            if (status.state != SessionStatus.State.WAITING_FOR_DATA) status.ready();
        }
        //获得多行输入结果
        else if (status.state == SessionStatus.State.WAITING_FOR_DATA) {
            if (buffer.readableBytes() >= status.bytesNeeded + MemcachedResponseEncoder.CRLF.capacity()) {

                // verify delimiter matches at the right location
                ChannelBuffer dest = buffer.slice(buffer.readerIndex() + status.bytesNeeded, MIN_BYTES_LINE);

                if (!dest.equals(MemcachedResponseEncoder.CRLF)) {
                    // before we throw error... we're ready for the next command
                    status.ready();

                    // error, no delimiter at end of payload
                    throw new IncorrectlyTerminatedPayloadException("payload not terminated correctly");
                } else {
                    status.processingMultiline();

                    // There's enough bytes in the buffer and the delimiter is at the end. Read it.
                    ChannelBuffer result = buffer.slice(buffer.readerIndex(), status.bytesNeeded);

                    buffer.skipBytes(status.bytesNeeded + MemcachedResponseEncoder.CRLF.capacity());

                    CommandMessage commandMessage = continueSet(channel, status, result, ctx);

                    if (status.state != SessionStatus.State.WAITING_FOR_DATA) status.ready();

                    return commandMessage;
                }
            }
        } else {
            throw new InvalidProtocolStateException("invalid protocol state");
        }
        return null;
    }

    /**
     * Process an individual complete protocol line and either passes the command for processing by the
     * session handler, or (in the case of SET-type commands) partially parses the command and sets the session into
     * a state to wait for additional data.
     *
     * @param parts                 the (originally space separated) parts of the command
     * @param channel               the netty channel to operate on
     * @param channelHandlerContext the netty channel handler context
     * @throws
     * @throws
     */
    private Object processLine(List<ChannelBuffer> parts, Channel channel, ChannelHandlerContext channelHandlerContext) throws
                                                                                                                        UnknownCommandException, MalformedCommandException {
        final int numParts = parts.size();

        // Turn the command into an enum for matching on
        Op op;
        try {
            op = Op.FindOp(parts.get(0));
            if (op == null)
                throw new IllegalArgumentException("unknown operation: " + parts.get(0).toString());
        } catch (IllegalArgumentException e) {
            throw new UnknownCommandException("unknown operation: " + parts.get(0).toString());
        }

        // Produce the initial command message, for filling in later
        CommandMessage cmd = CommandMessage.command(op);


        switch (op) {
            case DELETE:
                cmd.setKey(parts.get(1));

                if (numParts >= MIN_BYTES_LINE) {
                    if (parts.get(numParts - 1).equals(NOREPLY)) {
                        cmd.noreply = true;
                        if (numParts == 4)
                            cmd.time = BufferUtils.atoi(parts.get(MIN_BYTES_LINE));
                    } else if (numParts == 3)
                        cmd.time = BufferUtils.atoi(parts.get(MIN_BYTES_LINE));
                }

                return cmd;

            case SET:
            case CAS:
                if (numParts < 5) {
                    throw new MalformedCommandException("invalid command length");
                }

                //
                int size = BufferUtils.atoi(parts.get(4));
                //获得过期时间
                long expire = BufferUtils.atoi(parts.get(3)) * 1000;
                int flags = BufferUtils.atoi(parts.get(MIN_BYTES_LINE));
                cmd.element = new LocalCacheElement(new Key(parts.get(1).slice()), flags, expire != 0 && expire < CacheElement.THIRTY_DAYS ? LocalCacheElement.Now() + expire : expire, 0L);

                // 填充cas和noreply
                if (numParts > 5) {
                    int noreply = op == Op.CAS ? 6 : 5;
                    if (op == Op.CAS) {
                        cmd.cas_key = BufferUtils.atol(parts.get(5));
                    }

                    if (numParts == noreply + 1 && parts.get(noreply).equals(NOREPLY))
                        cmd.noreply = true;
                }

                // Now indicate that we need more for this command by changing the session status's state.
                // This instructs the frame decoder to start collecting data for us.
                status.needMore(size, cmd);
                break;

            //
            case GET:
            case GETS:
                // Get all the keys
                cmd.setKeys(parts.subList(1, numParts));

                // Pass it on.
                return cmd;
            default:
                throw new UnknownCommandException("unknown command: " + op);
        }

        return null;
    }

    /**
     * Handles the continuation of a SET/ADD/REPLACE command with the data it was waiting for.
     *
     * @param channel               netty channel
     * @param state                 the current session status (unused)
     * @param remainder             the bytes picked up
     * @param channelHandlerContext netty channel handler context
     */
    private CommandMessage continueSet(Channel channel, SessionStatus state, ChannelBuffer remainder, ChannelHandlerContext channelHandlerContext) {
        state.cmd.element.setData(remainder);
        return state.cmd;
    }
}
