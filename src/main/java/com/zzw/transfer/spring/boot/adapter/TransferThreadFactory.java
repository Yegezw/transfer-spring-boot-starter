package com.zzw.transfer.spring.boot.adapter;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.WorkProcessor;
import com.zzw.transfer.spring.boot.adapter.handler.MultiThreadHandlerAdapter;
import com.zzw.transfer.spring.boot.adapter.handler.SingleThreadHandlerAdapter;
import com.zzw.transfer.spring.boot.adapter.monitor.SingleThreadMonitorAdapter;
import com.zzw.transfer.spring.boot.adapter.saver.MultiThreadSaverAdapter;
import com.zzw.transfer.spring.boot.adapter.saver.SingleThreadSaverAdapter;

import java.lang.reflect.Field;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("all")
public class TransferThreadFactory implements ThreadFactory
{

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
                BatchEventProcessor batchEventProcessor = BatchEventProcessor.class.cast(r);
                Object              eventHandler        = EVENT_HANDLER.get(batchEventProcessor);
                if (eventHandler.getClass() == SingleThreadHandlerAdapter.class)
                {
                    thread.setName("数据同步-单线程处理");
                }
                else if (eventHandler.getClass() == SingleThreadSaverAdapter.class)
                {
                    thread.setName("数据同步-单线程保存");
                }
                else if (eventHandler.getClass() == SingleThreadMonitorAdapter.class)
                {
                    thread.setName("数据同步-单线程监控");
                }
            }
            // 多线程
            else if (r.getClass() == WorkProcessor.class)
            {
                WorkProcessor workProcessor = WorkProcessor.class.cast(r);
                Object        workHandler   = WORK_HANDLER.get(workProcessor);
                if (workHandler.getClass() == MultiThreadHandlerAdapter.class)
                {
                    thread.setName("数据同步-多线程处理-" + handlerCount.getAndAdd(1));
                }
                else if (workHandler.getClass() == MultiThreadSaverAdapter.class)
                {
                    thread.setName("数据同步-多线程保存-" + saverCount.getAndAdd(1));
                }
            }
        }
        catch (IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
        return thread;
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
