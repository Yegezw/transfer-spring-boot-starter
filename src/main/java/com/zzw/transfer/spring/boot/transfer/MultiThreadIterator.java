package com.zzw.transfer.spring.boot.transfer;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class MultiThreadIterator<E> implements Iterable<E>, Iterator<E>, Closeable
{

    private Iterator<E> it;

    @Override
    public Iterator<E> iterator()
    {
        return this;
    }

    @Override
    public boolean hasNext()
    {
        try
        {
            // (1) 未启动
            if (it == null)
            {
                start();
            }

            // (3) 有数据
            if (it != null && it.hasNext())
            {
                return true;
            }

            // (2) 无数据
            List<E> list;
            for (; ; )
            {
                if (activeThreadNum.get() == 0)
                {
                    // (4) 没有线程, 不必等待
                    list = queue.poll();
                    if (list != null)
                    {
                        it = list.iterator();
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
                else
                {
                    // (2) 还有线程, 需要等待
                    // 为什么不用 take() 而用 for + poll() ?
                    // 因为最后一个线程拿到 null / 空集合时, 不会往 queue 中添加数据, take() 就会一直阻塞
                    // 而 for + poll() 可以在 200 ms 后 for 自旋, 判断是否还有线程在工作, 从而决定是否继续等待
                    list = queue.poll(200L, TimeUnit.MILLISECONDS);
                    if (list != null)
                    {
                        it = list.iterator();
                        return true;
                    }
                }
            }
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException("MultiThreadIterator 获取数据期间被中断", e);
        }
    }

    @Override
    public E next()
    {
        return it.next();
    }

    // ------------------------------------------------

    /*
     * 理想的扩展点
     * 1、获取方式 -> DB? HTTP? MQ?
     * 2、获取参数 -> 非 id 字段? 个性化获取参数?
     * 3、获取策略 -> 按需获取以节省内存?
     */

    private final int           index;
    private final int           stride;
    private final AtomicInteger startIndex;

    private final Executor      executor;
    private final int           threadNum;
    private final AtomicInteger activeThreadNum;

    private final BlockingQueue<List<E>> queue;

    public MultiThreadIterator(int startIndex, int stride, int threadNum, Executor executor)
    {
        this(startIndex, stride, threadNum, executor, new LinkedBlockingQueue<>());
    }

    public MultiThreadIterator(int startIndex, int stride, int threadNum, Executor executor, BlockingQueue<List<E>> queue)
    {
        this.index           = startIndex;
        this.startIndex      = new AtomicInteger(index);
        this.stride          = stride;
        this.threadNum       = threadNum;
        this.activeThreadNum = new AtomicInteger(0);
        this.executor        = executor;
        this.queue           = queue; // TODO jctools.MPSCQueue ?
    }

    // ------------------------------------------------

    private void start()
    {
        for (int i = 0; i < threadNum; i++)
        {
            executor.execute(new CyclicFetch());
        }
        activeThreadNum.set(threadNum);
    }

    @Override
    public void close()
    {
        it = null;
        startIndex.set(index);
    }

    /**
     * 无数据时返回 null OR 空集合
     */
    protected abstract List<E> fetchData(int l, int r);

    private class CyclicFetch implements Runnable
    {
        @Override
        public void run()
        {
            for (; ; )
            {
                int l = startIndex.getAndAdd(stride);

                List<E> data = fetchData(l, l + stride - 1);
                if (data == null || data.isEmpty())
                {
                    break; // 不允许添加 null 和 空集合
                }

                // queue.add(data);
                try
                {
                    queue.put(data);
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException("MultiThreadIterator.CyclicFetch 获取数据期间被中断", e);
                }
            }

            activeThreadNum.getAndAdd(-1);
        }
    }
}
