####memcached-demo
* 首先十分抱歉这个编程题目拖这么久,最近这段时间由于线上迭代开发较多，很难抽出时间去做.
* 这里的实现借鉴jmemcached,在此基础上只实现了set,get等基本命令.
* 缓存存储使用的是Concurrentlinkedhashmap,缓存抛弃策略为FIFO.
* 测试类为MemCacheServerTest,测试用例包括二进制与文本协议.
