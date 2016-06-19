package com.alipay.zixiao;

import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.MemcachedClient;
import org.junit.After;
import org.junit.Before;
import org.junit.ComparisonFailure;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * 使用spymemcached进行测试
 */
@RunWith(Parameterized.class)
public class MemCacheServerTest extends AbstractCacheTest {

    /**
     * 单线程
     */
    private MemcachedClient   singleClient;
    private InetSocketAddress address;
    /**
     * 多线程读线程1
     */
    private MemcachedClient   multiClient1;

    /**
     * 多线程读线程2
     */
    private MemcachedClient   multiClient2;

    /**
     * 多线程通信cas
     */
    private long cas;
    /**
     * 期望值
     */
    private String expectValue=VALUE;

    /**
     * 线程通信
     */
    private boolean isOk=false;


    protected static final String KEY = "MyKey";

    protected static final String VALUE = "MyValue";

    protected static final int TWO_WEEKS = 1209600; // 60*60*24*14 = 1209600

    public MemCacheServerTest(ProtocolMode protocolMode) {
        super(protocolMode);
    }

    @Before public void setUp() throws Exception {
        super.setup();

        this.address = new InetSocketAddress("localhost", getPort());
        if (getProtocolMode() == ProtocolMode.BINARY) {
            singleClient = new MemcachedClient(new BinaryConnectionFactory(),
                    Arrays.asList(address));
            multiClient1 = new MemcachedClient(new BinaryConnectionFactory(), Arrays.asList(address));
            multiClient2 = new MemcachedClient(new BinaryConnectionFactory(), Arrays.asList(address));
        }
        else {
            singleClient = new MemcachedClient(Arrays.asList(address));
            multiClient1 = new MemcachedClient( Arrays.asList(address));
            multiClient2 = new MemcachedClient( Arrays.asList(address));

        }
    }

    @After public void tearDown() throws Exception {
        if (singleClient != null)
            singleClient.shutdown();
        if(multiClient1 != null){
            multiClient1.shutdown();
        }
        if(multiClient2 != null){
            multiClient2.shutdown();
        }

    }

    /**
     *  测试正常的get与set 存储 String类型
     */
    @Test
    public void testGetSet() throws IOException, InterruptedException, ExecutionException {
        Future<Boolean> future = singleClient.set(KEY, TWO_WEEKS, VALUE);
        assertTrue(future.get());
        assertEquals(VALUE, singleClient.get(KEY));
    }



    /**
     *  测试JAVA POJO对象
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testPOJOObject()
            throws ExecutionException, InterruptedException {

        User user = new User("zixiao", 26);

        Future<Boolean> future = singleClient.set(KEY, TWO_WEEKS, user);
        assertTrue(future.get());
        User userValue = (User) singleClient.get(KEY);
        assertEquals("zixiao", userValue.getName());
        assertEquals(26, userValue.getAge());


    }

    /**
     *  测试多线程读取
     *  1.一个线程存储,两个线程读取
     *  2.两个线程存储同一个key
     *  3.一个线程存储,另一个线程读取
     */
    @Test
    public void testMultiThreadRead () throws Exception{


        Future<Boolean> future = singleClient.set(KEY, TWO_WEEKS, VALUE);
        assertTrue(future.get());


        Thread readThread1 = new Thread(new Runnable() {
            public void run() {
                try {
                    assertEquals(VALUE, multiClient1.get(KEY));
                }
                //通过捕获线程内异常来判断测试用例是否真正通过
                catch (ComparisonFailure cf){
                    expectValue = "wrong";
                }

            }
        });
        Thread readThread2 = new Thread(new Runnable() {
            public void run() {
                try {
                    assertEquals(VALUE, multiClient2.get(KEY));
                }
                //通过捕获线程内异常来判断测试用例是否真正通过
                catch (ComparisonFailure cf){
                    expectValue = "wrong";
                }
            }
        });

        readThread1.start();
        readThread2.start();

        Thread.sleep(1000);
        assertEquals(VALUE,expectValue);


    }

    /**
     * 测试多线程写入
     * @throws Exception
     */
    @Test
    public void testMultiThreadWrite () throws Exception{


        Future<Boolean> future = singleClient.set(KEY, TWO_WEEKS, VALUE);
        assertTrue(future.get());

        Thread thread1 = new Thread(new Runnable() {
            public void run() {


                    try {
                        CASValue<Object> casValue = multiClient1.gets(KEY);
                        assertEquals(casValue.getValue(), VALUE);
                        cas = casValue.getCas();
                        CASResponse casResponse = multiClient1.cas(KEY, cas, TWO_WEEKS, VALUE);
                        isOk = true;
                        assertEquals(CASResponse.OK, casResponse);
                    }
                    //通过捕获线程内错误来判断测试用例是否真正通过
                    catch (Error error) {
                        error.printStackTrace();
                        expectValue = "wrong";
                    }


            }
        });
        final Thread thread2 = new Thread(new Runnable() {
            public void run() {


                    try {
                        //取更新线程1已经更新的值
                        while (!isOk){
                        }
                        CASResponse casResponse = multiClient2.cas(KEY, cas, TWO_WEEKS, VALUE);
                        assertEquals(CASResponse.EXISTS, casResponse);
                    }//通过捕获线程内错误来判断测试用例是否真正通过
                    catch (Error error) {
                        error.printStackTrace();
                        expectValue = "wrong";
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

            }
        });

//        thread1.start();
//        thread2.start();
        thread1.run();
        thread2.run();

        Thread.sleep(2000);
        assertEquals(VALUE,expectValue);

    }





    protected static Object getBigObject() {
        final Map<String, Double> map = new HashMap<String, Double>();

        for (int i = 0; i < 13000; i++) {
            map.put(Integer.toString(i), i / 42.0);
        }

        return map;
    }

}
