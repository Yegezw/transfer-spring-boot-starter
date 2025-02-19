package com.zzw.transfer.spring.boot.transfer;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.lmax.disruptor.RingBuffer;
import com.mysql.cj.exceptions.DeadlockTimeoutRollbackMarker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.AopContext;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 转移器: 生产 -> 处理 -> 收集 -> 保存
 */
public abstract class Transfer<S, T>
{

    private static final Logger log = LoggerFactory.getLogger(Transfer.class);

    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * 因为多线程插入产生死锁而导致插入失败的数据
     */
    private final ConcurrentLinkedQueue<List<S>> deadlockData = new ConcurrentLinkedQueue<>();

    private RingBuffer<Bucket> ringBuffer;
    private TransferRepository transferRepository;

    public void setDisruptor(RingBuffer<Bucket> ringBuffer)
    {
        this.ringBuffer = ringBuffer;
    }

    public void setTransferRepository(TransferRepository transferRepository)
    {
        this.transferRepository = transferRepository;
    }

    /**
     * 获取该转移器的唯一标识
     *
     * @return 唯一标识
     */
    protected abstract Object getMark();

    // ------------------------------------------------

    public abstract void postProcessBeforeStart(Object startupParam);

    public abstract void postProcessAfterStart(Object startupParam);

    /**
     * 线程安全的启动
     *
     * @return {@code true} 数据发布成功
     * <br>{@code false} {@link Transfer#fetchData(Object)} 获取数据异常 / {@link Transfer#fetchData(Object)} 返回 null
     */
    @Transactional
    public boolean start(Object startupParam)
    {
        final String className = getRealClassName();

        if (started.compareAndSet(false, true)) transferRepository.start(getMark());
        else throw new RuntimeException(getMark() + " 已启动, 不可重复启动");

        Iterable<S> all = null;
        try
        {
            all = fetchData(startupParam);
            if (all != null)
            {
                publish(all, true);
                return true;
            }
            else
            {
                log.error("{} {} 获取数据为 null", className, getMark());
                return false;
            }
        }
        catch (Exception e)
        {
            log.error("{} {} 获取数据异常", className, getMark(), e);
            return false;
        }
        finally
        {
            started.set(false);
            if (all != null) close(all);
        }
    }

    protected void close(Iterable<S> all)
    {
        if (all instanceof Closeable c)
        {
            try
            {
                c.close();
            }
            catch (IOException e)
            {
                log.error("{} {} 数据流关闭失败", getRealClassName(), getMark(), e);
            }
        }
    }

    /**
     * 空的迭代器也应该发布一次
     */
    protected void publish(Iterable<S> all, boolean careLastPublish)
    {
        Iterator<S> it         = all.iterator();
        int         bucketSize = getBucketSize();

        boolean emptyIterator = !(it.hasNext());
        if (careLastPublish)
        {
            // 空的迭代器
            if (emptyIterator)
            {
                doPublish(new ArrayList<>(0), true);
                return;
            }

            // 非空迭代器
            ArrayList<S> data = new ArrayList<>(bucketSize);
            while (it.hasNext())
            {
                data.add(it.next());
                if (data.size() == bucketSize)
                {
                    if (it.hasNext())
                    {
                        doPublish(data, false);
                        data = new ArrayList<>(bucketSize);
                    }
                    else
                    {
                        doPublish(data, true);
                        data = new ArrayList<>(0);
                    }
                }
            }
            if (!data.isEmpty()) doPublish(data, true);
        }
        else
        {
            // 空的迭代器
            if (emptyIterator)
            {
                doPublish(new ArrayList<>(0), false);
                return;
            }

            // 非空迭代器
            ArrayList<S> data = new ArrayList<>(bucketSize);
            while (it.hasNext())
            {
                data.add(it.next());
                if (data.size() == bucketSize)
                {
                    doPublish(data, false);
                    data = new ArrayList<>(bucketSize);
                }
            }
            if (!data.isEmpty()) doPublish(data, false);
        }
    }

    /**
     * 调用者需要确保 data != null && data.size != 0, 除非是空的迭代器
     *
     * @param data        数据
     * @param lastPublish 是否为最后一批数据
     */
    protected void doPublish(List<S> data, boolean lastPublish)
    {
        final long   sequence = ringBuffer.next();
        final Bucket bucket   = ringBuffer.get(sequence);
        try
        {
            bucket.setMark(getMark());
            bucket.setData(data);
            bucket.setLastPublish(lastPublish);
            if (log.isInfoEnabled())
            {
                log.info("{} {} 发布数据 {} 条", getRealClassName(), getMark(), data.size());
            }
        }
        finally
        {
            ringBuffer.publish(sequence);
        }
    }

    protected abstract int getBucketSize();

    protected abstract Iterable<S> fetchData(Object startupParam);

    // ------------------------------------------------

    /**
     * 空的迭代器也应该处理一次
     */
    @SuppressWarnings("unchecked")
    public void handle(Bucket bucket)
    {
        if (!shouldHandle())
        {
            List<S> data = bucket.getData();
            bucket.setHandledData(data);
            return;
        }

        boolean      lastPublish = bucket.isLastPublish();
        List<S>      data        = bucket.getData();
        List<T>      handledData = new ArrayList<>(data.size());
        List<Object> errorInfo   = new ArrayList<>(5);

        // 空的迭代器
        if (data.isEmpty() && lastPublish)
        {
            try
            {
                List<T> target = doHandle(null, true);
                if (target != null) handledData.addAll(target);
            }
            catch (Exception e)
            {
                errorInfo.add(getHandleErrorInfo(null, e));
            }
        }
        // 非空迭代器
        else
        {
            boolean lastData = false;
            for (int i = 0; i < data.size(); i++)
            {
                if (lastPublish && i == data.size() - 1)
                {
                    lastData = true;
                }
                S source = data.get(i);
                try
                {
                    List<T> target = doHandle(source, lastData);
                    if (target != null) handledData.addAll(target);
                }
                catch (Exception e)
                {
                    errorInfo.add(getHandleErrorInfo(source, e));
                }
            }
        }

        bucket.setHandledData(handledData);
        final String className = getRealClassName();
        if (log.isInfoEnabled())
        {
            log.info("{} {} 处理数据 {} 条 -> {} 条, 失败 {} 条", className, getMark(), data.size(), handledData.size(), errorInfo.size());
        }
        if (!errorInfo.isEmpty())
        {
            log.error("{} {} 失败数据 {} 条, 异常信息: {}", className, getMark(), errorInfo.size(), errorInfo);
        }
    }

    protected abstract boolean shouldHandle();

    protected abstract List<T> doHandle(S source, boolean lastData);

    /**
     * 获取处理数据异常信息
     */
    protected abstract Object getHandleErrorInfo(S source, Exception e);

    // ------------------------------------------------

    /**
     * handledData 不是空集合才会收集
     */
    @SuppressWarnings("unchecked")
    public void collect(Bucket bucket)
    {
        if (!shouldCollectAfterHandle() || bucket.getHandledData().isEmpty())
        {
            return;
        }

        boolean         lastPublish   = bucket.isLastPublish();
        List<T>         handledData   = bucket.getHandledData();
        List<T>         collectedData = new ArrayList<>(1);
        List<Exception> errorInfo     = new ArrayList<>(5);

        boolean lastData = false;
        for (int i = 0; i < handledData.size(); i++)
        {
            if (lastPublish && i == handledData.size() - 1)
            {
                lastData = true;
            }
            T handled = handledData.get(i);
            try
            {
                List<T> collected = doCollect(handled, lastData);
                if (collected != null) collectedData.addAll(collected);
            }
            catch (Exception e)
            {
                errorInfo.add(e);
            }
        }

        bucket.setHandledData(collectedData);
        if (!errorInfo.isEmpty())
        {
            log.error("{} {} 失败数据 {} 条, 异常信息: {}", getRealClassName(), getMark(), errorInfo.size(), errorInfo);
        }
    }

    protected abstract boolean shouldCollectAfterHandle();

    protected abstract List<T> doCollect(T target, boolean lastData);

    // ------------------------------------------------

    /**
     * handledData 不是空集合才会保存
     */
    @SuppressWarnings("unchecked")
    public void save(Bucket bucket)
    {
        List<S> data        = bucket.getData();
        List<T> handledData = bucket.getHandledData();
        try
        {
            if (!handledData.isEmpty())
            {
                Transfer<S, T> proxy = (Transfer<S, T>) AopContext.currentProxy();
                int            rows  = proxy.doSave(handledData);
                if (log.isInfoEnabled())
                {
                    log.info("{} {} 保存数据 {} 条", getRealClassName(), getMark(), rows);
                }
            }
        }
        catch (Exception e)
        {
            bucket.setData(new ArrayList<>(0));
            bucket.setHandledData(new ArrayList<>(0));
            Throwable cause = Throwables.getRootCause(e);
            if (cause instanceof DeadlockTimeoutRollbackMarker)
            {
                deadlockData.add(data);
                log.error("{} {} 保存数据 {} 条发生死锁, 当前死锁数据共 {} 条", getRealClassName(), getMark(), handledData.size(), deadlockData.size());
            }
            else saveFail(data, handledData, e);
        }
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    protected abstract int doSave(List<T> handledData);

    protected abstract void saveFail(List<S> data, List<T> handledData, Exception e);

    /**
     * 非线程安全, 用于死锁数据重新发布<br>
     * 只有数据走过一轮后才能判断是否有死锁数据, 因此该方法不能在 {@link Transfer#start(Object)} 调用
     */
    public boolean rePublishDeadlockData()
    {
        if (deadlockData.isEmpty()) return false;

        ArrayList<List<S>> temp = new ArrayList<>(deadlockData);
        ArrayList<List<S>> data = new ArrayList<>(temp.size());
        for (List<S> list : temp)
        {
            data.addAll(Lists.partition(list, 100));
        }

        deadlockData.clear();
        transferRepository.start(getMark());

        for (int i = 0; i < data.size(); i++)
        {
            List<S> list = data.get(i);
            doPublish(list, i == data.size() - 1);
        }
        return true;
    }

    // ------------------------------------------------

    @Override
    public String toString()
    {
        return getMark().toString();
    }

    public String getRealClassName()
    {
        // 该类可能被代理, 因此需要截断不必要的代理名称
        String className = getClass().getSimpleName();
        int    endIndex  = className.indexOf('$');
        return endIndex == -1 ? className : className.substring(0, endIndex);
    }
}
