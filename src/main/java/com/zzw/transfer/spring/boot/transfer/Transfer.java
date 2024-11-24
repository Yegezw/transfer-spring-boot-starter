package com.zzw.transfer.spring.boot.transfer;

import com.lmax.disruptor.RingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 转移器: 生产 -> 处理 -> 保存
 */
public abstract class Transfer<S, T>
{

    private static final Logger log = LoggerFactory.getLogger(Transfer.class);

    private RingBuffer<Bucket> ringBuffer;

    public void setDisruptor(RingBuffer<Bucket> ringBuffer)
    {
        this.ringBuffer = ringBuffer;
    }

    protected abstract Object getMark();

    // ------------------------------------------------

    @Transactional(readOnly = true)
    public void start() throws IOException
    {
        int bucketSize = getBucketSize();

        Iterable<S>  all  = getDate();
        ArrayList<S> data = new ArrayList<>();
        for (S source : all)
        {
            data.add(source);
            if (data.size() == bucketSize)
            {
                publish(data);
                data = new ArrayList<>(bucketSize);
            }
        }
        if (!data.isEmpty()) publish(data);

        if (all instanceof Closeable c) c.close();
    }

    private void publish(List<S> data)
    {
        final long   sequence = ringBuffer.next();
        final Bucket bucket   = ringBuffer.get(sequence);
        try
        {
            bucket.setData(data);
            bucket.setMark(getMark());
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

    protected abstract Iterable<S> getDate();

    // ------------------------------------------------

    @SuppressWarnings("all")
    final void handle(Bucket bucket)
    {
        List data      = bucket.getData();
        List newData   = new ArrayList();
        List errorData = null;

        for (int i = 0; i < data.size(); i++)
        {
            S source = (S) data.get(i);
            data.set(i, null); // help gc
            try
            {
                List<T> target = doHandle(source);
                newData.addAll(target);
            }
            catch (Throwable throwable)
            {
                if (errorData == null)
                {
                    errorData = new ArrayList();
                }
                errorData.add(getHandleErrorTrack(source));
            }
        }

        bucket.setData(newData);
        if (log.isInfoEnabled())
        {
            log.info("{} 处理数据 {} 条 -> {} 条, 失败 {} 条", getMark(), data.size(), newData.size(), errorData == null ? 0 : errorData.size());
        }
        if (log.isInfoEnabled() && errorData != null && errorData.size() != 0)
        {
            log.error("{} 失败数据 {} 条, 追踪信息: {}", getMark(), errorData.size(), errorData);
        }
    }

    protected abstract List<T> doHandle(S source);

    /**
     * 获取处理数据异常追踪信息
     */
    protected abstract Object getHandleErrorTrack(S source);

    // ------------------------------------------------

    @SuppressWarnings("all")
    final void save(Bucket bucket)
    {
        if (this.getMark() == bucket.getMark())
        {
            List data = bucket.getData();
            int  rows = doSave(data);
            if (log.isInfoEnabled())
            {
                log.info("{} 保存数据 {} 条", getMark(), rows);
            }
            bucket.clear(); // help gc
        }
    }

    protected abstract int doSave(List<T> data);
}
