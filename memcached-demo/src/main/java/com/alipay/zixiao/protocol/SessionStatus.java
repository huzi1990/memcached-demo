package com.alipay.zixiao.protocol;

import java.io.Serializable;

/**
 * 目前的session状态
 */
public final class SessionStatus implements Serializable {

    /**
     * 枚举
     */
    public static enum State {
        WAITING_FOR_DATA,
        READY,
        PROCESSING_MULTILINE,
    }

    // 进入的session数量
    public State state;

    // 等待数据量
    public int bytesNeeded;


    public CommandMessage cmd;


    public SessionStatus() {
        ready();
    }

    public SessionStatus ready() {
        this.cmd = null;
        this.bytesNeeded = -1;
        this.state = State.READY;

        return this;
    }

    public SessionStatus processingMultiline() {
        this.state = State.PROCESSING_MULTILINE;

        return this;
    }

    public SessionStatus needMore(int size, CommandMessage cmd) {
        this.cmd = cmd;
        this.bytesNeeded = size;
        this.state = State.WAITING_FOR_DATA;

        return this;
    }

}
