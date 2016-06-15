package com.alipay.zixiao.cache;

import com.alipay.zixiao.cache.hash.SizedItem;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.Serializable;

/**
 */
public interface CacheElement extends Serializable, SizedItem {
    public final static long THIRTY_DAYS = 2592000000L;

    int size();

    int hashCode();

    long getExpire();

    int getFlags();

    ChannelBuffer getData();

    void setData(ChannelBuffer data);

    Key getKey();

    long getCasUnique();

    void setCasUnique(long casUnique);

    boolean isBlocked();

    void block(long blockedUntil);

    long getBlockedUntil();

    CacheElement append(LocalCacheElement element);

    CacheElement prepend(LocalCacheElement element);

    LocalCacheElement.IncrDecrResult add(int mod);
}
