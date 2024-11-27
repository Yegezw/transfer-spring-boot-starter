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
    public void start() throws IOException
    {
        if (started.compareAndSet(false, true)) transferRepository.start(getMark());
        else throw new RuntimeException(getMark() + " 已启动, 不可重复启动");

        Iterable<S> all = null;
        try
        {
            all = getData();
            publish(all);
        }
        catch (Exception e)
        {
            log.error("{} 启动错误, 获取数据失败", getMark(), e);
        }
        finally
        {
            started.set(false);
            close(all);
        }
    }

    private void close(Iterable<S> all)
    {
        if (all == null) return;
        if (all instanceof Closeable)
        {
            Closeable c = (Closeable) all;
            try
            {
                c.close();
            }
            catch (IOException e)
            {
                log.error("{} 数据流复发关闭", getMark(), e);
            }
        }
    }

    private void publish(Iterable<S> all)
    {
        if (all == null)
        {
            log.info("{} 发布数据 {} 条", getMark(), 0);
            return;
        }
        int bucketSize = getBucketSize();

        ArrayList<S> data1 = new ArrayList<>(bucketSize);
        ArrayList<S> data2 = new ArrayList<>(bucketSize);

        // data2 满时发布 data1
        for (S source : all)
        {
            if (data1.size() < bucketSize)
            {
                data1.add(source);
            }
            else if (data2.size() < bucketSize)
            {
                data2.add(source);
                if (data2.size() == bucketSize)
                {
                    publish(data1, false);
                    data1 = data2;
                    data2 = new ArrayList<>(bucketSize);
                }
            }
        }

        if (data2.isEmpty())
        {
            publish(data1, true);
        }
        else
        {
            publish(data1, false);
            publish(data2, true);
        }
    }

    private void publish(List<S> data, boolean lastPublish)
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
    public final void save(Bucket bucket)
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
