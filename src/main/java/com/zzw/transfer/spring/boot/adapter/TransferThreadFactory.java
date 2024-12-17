package com.zzw.transfer.spring.boot.adapter;

import com.google.common.collect.ImmutableMap;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.WorkProcessor;
import com.zzw.transfer.spring.boot.adapter.collector.SingleThreadCollectorAdapter;
import com.zzw.transfer.spring.boot.adapter.handler.MultiThreadHandlerAdapter;
import com.zzw.transfer.spring.boot.adapter.handler.SingleThreadHandlerAdapter;
import com.zzw.transfer.spring.boot.adapter.monitor.SingleThreadMonitorAdapter;
import com.zzw.transfer.spring.boot.adapter.saver.MultiThreadSaverAdapter;
import com.zzw.transfer.spring.boot.adapter.saver.SingleThreadSaverAdapter;
import com.zzw.transfer.spring.boot.transfer.Bucket;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class TransferThreadFactory implements ThreadFactory
{

    private static final Map<Class<?>, String> THREAD_NAME = ImmutableMap.of(
            SingleThreadHandlerAdapter.class, "数据同步-单线程处理",
            SingleThreadCollectorAdapter.class, "数据同步-单线程收集",
            SingleThreadSaverAdapter.class, "数据同步-单线程保存",
            SingleThreadMonitorAdapter.class, "数据同步-单线程监控"
    );

    private final AtomicInteger handlerCount = new AtomicInteger(1);
    private final AtomicInteger saverCount   = new AtomicInteger(1);

    @Override
    public Thread newThread(Runnable r)
    {
        Thread thread = new Thread(r);
        try
        {
            // 单线程
            if (r.getClass() == BatchEventProcessor.class)
            {
                setThreadNameForBatchEventProcessor(thread, r);
            }
            // 多线程
            else if (r.getClass() == WorkProcessor.class)
            {
                setThreadNameForWorkProcessor(thread, r);
            }
        }
        catch (IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
        return thread;
    }

    @SuppressWarnings("unchecked")
    private void setThreadNameForBatchEventProcessor(Thread thread, Runnable r) throws IllegalAccessException
    {
        BatchEventProcessor<Bucket> batchEventProcessor = (BatchEventProcessor<Bucket>) r;
        Object                      eventHandler        = EVENT_HANDLER.get(batchEventProcessor);
        thread.setName(THREAD_NAME.get(eventHandler.getClass()));
    }

    @SuppressWarnings("unchecked")
    private void setThreadNameForWorkProcessor(Thread thread, Runnable r) throws IllegalAccessException
    {
        WorkProcessor<Bucket> workProcessor = (WorkProcessor<Bucket>) r;
        Object                workHandler   = WORK_HANDLER.get(workProcessor);
        if (workHandler.getClass() == MultiThreadHandlerAdapter.class)
        {
            thread.setName("数据同步-多线程处理-" + handlerCount.getAndAdd(1));
        }
        else if (workHandler.getClass() == MultiThreadSaverAdapter.class)
        {
            thread.setName("数据同步-多线程保存-" + saverCount.getAndAdd(1));
        }
    }

    private static final Field EVENT_HANDLER;
    private static final Field WORK_HANDLER;

    static
    {
        try
        {
            EVENT_HANDLER = BatchEventProcessor.class.getDeclaredField("eventHandler");
            EVENT_HANDLER.setAccessible(true);
            WORK_HANDLER = WorkProcessor.class.getDeclaredField("workHandler");
            WORK_HANDLER.setAccessible(true);
        }
        catch (final Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
