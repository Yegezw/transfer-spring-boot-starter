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

    private final BlockingQueue<List<E>> queue = new LinkedBlockingQueue<>();

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
            // 未启动
            if (it == null)
            {
                start();
            }

            // 有数据
            if (it != null && it.hasNext())
            {
                return true;
            }

            // 无数据
            List<E> list;
            for (; ; )
            {
                list = queue.poll(200L, TimeUnit.MILLISECONDS);
                if (list != null)
                {
                    it = list.iterator();
                    return true;
                }
                else if (activeThreadNum.get() == 0)
                {
                    // 必须双重检查, 但不必等待
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
     * 目前仅支持全量获取 + 多线程竞争不同的 id 段
     *
     * 待扩展
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

    public MultiThreadIterator(int stride, int startIndex, int threadNum, Executor executor)
    {
        this.index           = startIndex;
        this.stride          = stride;
        this.startIndex      = new AtomicInteger(index);
        this.threadNum       = threadNum;
        this.activeThreadNum = new AtomicInteger(0);
        this.executor        = executor;
    }

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

                queue.add(data);
            }

            activeThreadNum.getAndAdd(-1);
        }
    }
}
