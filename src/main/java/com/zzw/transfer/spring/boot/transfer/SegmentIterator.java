package com.zzw.transfer.spring.boot.transfer;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 段迭代器
 */
public abstract class SegmentIterator<E> implements Iterable<E>, Iterator<E>
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
            // 第一次
            if (it == null)
            {
                start();
                it = queue.take().iterator();
                return it.hasNext();
            }

            // 非第一次 + it 有数据
            if (it.hasNext())
            {
                return true;
            }

            // 非第一次 + it 无数据
            if (activeThreadNum.get() == 0)
            {
                return false;
            }
            it = queue.take().iterator();
            return it.hasNext();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException("SegmentIterator 获取数据期间被中断", e);
        }
    }

    @Override
    public E next()
    {
        return it.next();
    }

    // ------------------------------------------------

    private final int           index;
    private final int           stride;
    private final AtomicInteger startIndex;

    private final Executor      executor;
    private final int           threadNum;
    private final AtomicInteger activeThreadNum;

    public SegmentIterator(int stride, int startIndex, int threadNum, Executor executor)
    {
        this.index           = startIndex;
        this.stride          = stride;
        this.startIndex      = new AtomicInteger(index);
        this.threadNum       = threadNum;
        this.activeThreadNum = new AtomicInteger(-1);
        this.executor        = executor;
    }

    private void start()
    {
        reset();
        for (int i = 0; i < threadNum; i++)
        {
            executor.execute(new CyclicFetch());
        }
    }

    private void reset()
    {
        startIndex.set(index);
        activeThreadNum.set(threadNum);
    }

    /**
     * 通过 HTTP 获取数据, 无数据时返回 null OR 空集合
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
