package com.alipay.zixiao.cache.storage.bytebuffer;

/**
 */
public interface BlockStoreFactory<BS extends ByteBufferBlockStore> {
    BS manufacture(long sizeBytes, int blockSizeBytes);
}
