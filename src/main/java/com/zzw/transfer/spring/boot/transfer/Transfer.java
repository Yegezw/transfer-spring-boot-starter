package com.zzw.transfer.spring.boot.transfer;

import com.lmax.disruptor.RingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 转移器: 生产 -> 处理 -> 保存
 */
public abstract class Transfer<S, T>
{

    private static final Logger log = LoggerFactory.getLogger(Transfer.class);

    private final AtomicBoolean started = new AtomicBoolean(false);

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

    protected abstract Object getMark();

    // ------------------------------------------------

    @Transactional(readOnly = true)
    public void start()
    {
        if (started.compareAndSet(false, true)) transferRepository.start(getMark());
        else throw new RuntimeException(getMark() + " 已启动, 不可重复启动");

        Iterable<S> all = null;
        try
        {
            all = getData();
            if (all != null) publish(all);
            else log.error("{} 获取数据为 null", getMark());
        }
        catch (Exception e)
        {
            log.error("{} 获取数据异常", getMark(), e);
        }
        finally
        {
            started.set(false);
            if (all != null) close(all);
        }
    }

    private void close(Iterable<S> all)
    {
        if (all instanceof Closeable)
        {
            Closeable c = (Closeable) all;
            try
            {
                c.close();
            }
            catch (IOException e)
            {
                log.error("{} 数据流关闭失败", getMark(), e);
            }
        }
    }

    private void publish(Iterable<S> all)
    {
        int bucketSize = getBucketSize();

        Iterator<S>  it   = all.iterator();
        ArrayList<S> data = new ArrayList<>();
        while (it.hasNext())
        {
            data.add(it.next());
            if (data.size() == bucketSize)
            {
                if (it.hasNext())
                {
                    publish(data, false);
                    data = new ArrayList<>(bucketSize);
                }
                else
                {
                    publish(data, true);
                    data = new ArrayList<>(0);
                }
            }
        }

        if (!data.isEmpty()) publish(data, true);
    }

    private void publish(List<S> data, boolean lastPublish)
    {
        // caller need make sure data != null && data.size != 0
        final long   sequence = ringBuffer.next();
        final Bucket bucket   = ringBuffer.get(sequence);
        try
        {
            bucket.setMark(getMark());
            bucket.setData(data);
            bucket.setLastPublish(lastPublish);
            if (log.isInfoEnabled())
            {
                log.info("{} 发布数据 {} 条", getMark(), data.size());
            }
        }
        finally
        {
            ringBuffer.publish(sequence);
        }
    }

    protected abstract int getBucketSize();

    protected abstract Iterable<S> getData();

    // ------------------------------------------------

    @SuppressWarnings("all")
    public final void handle(Bucket bucket)
    {
        boolean      lastPublish = bucket.isLastPublish();
        List<S>      data        = bucket.getData();
        List<T>      newData     = new ArrayList<>();
        List<Object> errorInfo   = new ArrayList<>();

        boolean lastData = false;
        for (int i = 0; i < data.size(); i++)
        {
            if (lastPublish && i == data.size() - 1)
            {
                lastData = true;
            }
            S source = (S) data.get(i);
            data.set(i, null); // help gc
            try
            {
                List<T> target = doHandle(source, lastData);
                if (target != null) newData.addAll(target);
            }
            catch (Throwable throwable)
            {
                errorInfo.add(getHandleErrorInfo(source));
            }
        }

        bucket.setData(newData);
        if (log.isInfoEnabled())
        {
            log.info("{} 处理数据 {} 条 -> {} 条, 失败 {} 条", getMark(), data.size(), newData.size(), errorInfo.size());
            if (!errorInfo.isEmpty())
            {
                log.error("{} 失败数据 {} 条, 异常信息: {}", getMark(), errorInfo.size(), errorInfo);
            }
        }
    }

    protected abstract List<T> doHandle(S source, boolean lastData);

    /**
     * 获取处理数据异常信息
     */
    protected abstract Object getHandleErrorInfo(S source);

    // ------------------------------------------------

    @SuppressWarnings("all")
    public final void save(Bucket bucket)
    {
        List<T> data = bucket.getData();
        try
        {
            if (!data.isEmpty())
            {
                int rows = doSave(data);
                if (log.isInfoEnabled())
                {
                    log.info("{} 保存数据 {} 条", getMark(), rows);
                }
            }
        }
        catch (Exception e)
        {
            saveFail(data);
        }
    }

    protected abstract int doSave(List<T> data);

    protected void saveFail(List<T> data)
    {
        log.error("{} 保存 {} 条数据失败: {}", getMark(), data.size(), data);
    }
}
