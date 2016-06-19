
package com.alipay.zixiao.cache;

import com.alipay.zixiao.cache.storage.CacheStorage;
import org.jboss.netty.buffer.ChannelBuffers;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.*;

/**
 * 缓存的实现
 */
public final class CacheImpl extends AbstractCache<LocalCacheElement> implements Cache<LocalCacheElement> {

    final         CacheStorage<Key, LocalCacheElement> storage;
    final         DelayQueue<DelayedMCElement>         deleteQueue;
    private final ScheduledExecutorService             scavenger;

    public CacheImpl(CacheStorage<Key, LocalCacheElement> storage) {
        super();
        this.storage = storage;
        deleteQueue = new DelayQueue<DelayedMCElement>();

        scavenger = Executors.newScheduledThreadPool(1);
        scavenger.scheduleAtFixedRate(new Runnable(){
            public void run() {
                asyncEventPing();
            }
        }, 10, 2, TimeUnit.SECONDS);
    }


    public DeleteResponse delete(Key key, int time) {
        boolean removed = false;

        // 延期删除
        if (time != 0) {
            LocalCacheElement placeHolder = new LocalCacheElement(key, 0, 0, 0L);
            placeHolder.setData(ChannelBuffers.buffer(0));
            placeHolder.block(Now() + (long)time);

            storage.replace(key, placeHolder);

            deleteQueue.add(new DelayedMCElement(placeHolder));
        } else
            removed = storage.remove(key) != null;

        if (removed) return DeleteResponse.DELETED;
        else return DeleteResponse.NOT_FOUND;

    }


    public StoreResponse add(LocalCacheElement e) {
        final long origCasUnique = e.getCasUnique();
        e.setCasUnique(casCounter.getAndIncrement());
        final boolean stored = storage.putIfAbsent(e.getKey(), e) == null;
        //没存储成功恢复原来计数
        if (!stored) {
            e.setCasUnique(origCasUnique);
        }
        return stored ? StoreResponse.STORED : StoreResponse.NOT_STORED;
    }


    public StoreResponse replace(LocalCacheElement e) {
        return storage.replace(e.getKey(), e) != null ? StoreResponse.STORED : StoreResponse.NOT_STORED;
    }


    public StoreResponse append(LocalCacheElement element) {
        LocalCacheElement old = storage.get(element.getKey());
        if (old == null || isBlocked(old) || isExpired(old)) {
            getMisses.incrementAndGet();
            return StoreResponse.NOT_FOUND;
        }
        else {
            return storage.replace(old.getKey(), old, old.append(element)) ? StoreResponse.STORED : StoreResponse.NOT_STORED;
        }
    }


    public StoreResponse prepend(LocalCacheElement element) {
        LocalCacheElement old = storage.get(element.getKey());
        if (old == null || isBlocked(old) || isExpired(old)) {
            getMisses.incrementAndGet();
            return StoreResponse.NOT_FOUND;
        }
        else {
            return storage.replace(old.getKey(), old, old.prepend(element)) ? StoreResponse.STORED : StoreResponse.NOT_STORED;
        }
    }


    public StoreResponse set(LocalCacheElement e) {
        setCmds.incrementAndGet();//update stats

        e.setCasUnique(casCounter.getAndIncrement());

        storage.put(e.getKey(), e);

        return StoreResponse.STORED;
    }


    public StoreResponse cas(Long cas_key, LocalCacheElement e) {
        // 查看是否存在元素
        LocalCacheElement element = storage.get(e.getKey());
        if (element == null || isBlocked(element)) {
            getMisses.incrementAndGet();
            return StoreResponse.NOT_FOUND;
        }

        if (element.getCasUnique() == cas_key) {
            // 命中
        	e.setCasUnique(casCounter.getAndIncrement());
            if (storage.replace(e.getKey(), element, e)) return StoreResponse.STORED;
            else {
                getMisses.incrementAndGet();
                return StoreResponse.NOT_FOUND;
            }
        } else {
            // 并发处理,别的线程处理
            return StoreResponse.EXISTS;
        }
    }


    public Integer get_add(Key key, int mod) {
        LocalCacheElement old = storage.get(key);
        if (old == null || isBlocked(old) || isExpired(old)) {
            getMisses.incrementAndGet();
            return null;
        } else {
            LocalCacheElement.IncrDecrResult result = old.add(mod);
            return storage.replace(old.getKey(), old, result.replace) ? result.oldValue : null;
        }
    }


    protected boolean isBlocked(CacheElement e) {
        return e.isBlocked() && e.getBlockedUntil() > Now();
    }

    protected boolean isExpired(CacheElement e) {
        return e.getExpire() != 0 && e.getExpire() < Now();
    }


    public LocalCacheElement[] get(Key ... keys) {
        getCmds.incrementAndGet();

        LocalCacheElement[] elements = new LocalCacheElement[keys.length];
        int x = 0;
        int hits = 0;
        int misses = 0;
        for (Key key : keys) {
            LocalCacheElement e = storage.get(key);
            if (e == null || isExpired(e) || e.isBlocked()) {
                misses++;

                elements[x] = null;
            } else {
                hits++;

                elements[x] = e;
            }
            x++;

        }
        getMisses.addAndGet(misses);
        getHits.addAndGet(hits);

        return elements;

    }


    public boolean flush_all() {
        return flush_all(0);
    }


    public boolean flush_all(int expire) {
        storage.clear();
        return true;
    }


    public void close() throws IOException {
        scavenger.shutdown();;
        storage.close();
    }


    @Override
    protected Set<Key> keys() {
        return storage.keySet();
    }


    @Override
    public long getCurrentItems() {
        return storage.size();
    }


    @Override
    public long getLimitMaxBytes() {
        return storage.getMemoryCapacity();
    }


    @Override
    public long getCurrentBytes() {
        return storage.getMemoryUsed();
    }


    @Override
    public void asyncEventPing() {
        DelayedMCElement toDelete = deleteQueue.poll();
        if (toDelete != null) {
            storage.remove(toDelete.element.getKey());
        }
    }



    protected static class DelayedMCElement implements Delayed {
        private CacheElement element;

        public DelayedMCElement(CacheElement element) {
            this.element = element;
        }

        public long getDelay(TimeUnit timeUnit) {
            return timeUnit.convert(element.getBlockedUntil() - Now(), TimeUnit.MILLISECONDS);
        }

        public int compareTo(Delayed delayed) {
            if (!(delayed instanceof CacheImpl.DelayedMCElement))
                return -1;
            else
                return element.getKey().toString().compareTo(((DelayedMCElement) delayed).element.getKey().toString());
        }
    }
}
