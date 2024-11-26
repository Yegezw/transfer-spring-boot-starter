package com.zzw.transfer.spring.boot.transfer;

import com.lmax.disruptor.RingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
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
    private Dispatcher         dispatcher;

    public void setDisruptor(RingBuffer<Bucket> ringBuffer)
    {
        this.ringBuffer = ringBuffer;
    }

    public void setDispatcher(Dispatcher dispatcher)
    {
        this.dispatcher = dispatcher;
    }

    protected abstract Object getMark();

    // ------------------------------------------------

    @Transactional(readOnly = true)
    public void start() throws IOException
    {
        if (started.compareAndSet(false, true)) dispatcher.start(getMark());
        else throw new RuntimeException(getMark() + " 已启动, 不可重复启动");

        Iterable<S> all = null;
        try
        {
            int bucketSize = getBucketSize();

            all = getData();
            ArrayList<S> data       = new ArrayList<>();
            Bucket       lastBucket = null;
            for (S source : all)
            {
                data.add(source);
                if (data.size() == bucketSize)
                {
                    lastBucket = publish(data);
                    data       = new ArrayList<>(bucketSize);
                }
            }
            if (!data.isEmpty()) lastBucket = publish(data);

            // 虽然是发布完成后才 volatile 设置 lastPublish = ture
            // 但我不相信 MESI 的同步速度会比 MySQL 的写入速度还慢, 那真是太离谱了
            // 我甚至觉得 volatile 更新 lastPublish 都没有必要, 但由于每个数据流只设置一次, 并不会有太大损耗, 还是加上吧
            if (lastBucket != null) lastBucket.setLastPublish();
        }
        catch (Exception e)
        {
            log.error("{} 启动错误", getMark(), e);
        }
        finally
        {
            started.set(false);
            if (all instanceof Closeable c) c.close();
        }
    }

    private Bucket publish(List<S> data)
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
            return bucket;
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
        List data = bucket.getData();
        int  rows = doSave(data);
        if (log.isInfoEnabled())
        {
            log.info("{} 保存数据 {} 条", getMark(), rows);
        }
    }

    protected abstract int doSave(List<T> data);
}
