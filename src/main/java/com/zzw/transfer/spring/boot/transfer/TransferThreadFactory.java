package com.zzw.transfer.spring.boot.transfer;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.WorkProcessor;

import java.lang.reflect.Field;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("all")
public class TransferThreadFactory implements ThreadFactory
{

    private final AtomicInteger count = new AtomicInteger(1);

    @Override
    public Thread newThread(Runnable r)
    {
        Thread thread = new Thread(r);
        try
        {
            if (r.getClass() == BatchEventProcessor.class)
            {
                BatchEventProcessor batchEventProcessor = BatchEventProcessor.class.cast(r);
                Object              eventHandler        = EVENT_HANDLER.get(batchEventProcessor);
                if (eventHandler.getClass() == SingleHandlerWrapper.class)
                {
                    thread.setName("数据同步-单线程处理-" + count.getAndAdd(1));
                }
                else if (eventHandler.getClass() == SaverMonitor.class)
                {
                    thread.setName("数据同步-单线程监控-" + count.getAndAdd(1));
                }
            }
            else if (r.getClass() == WorkProcessor.class)
            {
                WorkProcessor workProcessor = WorkProcessor.class.cast(r);
                Object        workHandler   = WORK_HANDLER.get(workProcessor);
                if (workHandler.getClass() == MultiHandlerWrapper.class)
                {
                    thread.setName("数据同步-多线程处理-" + count.getAndAdd(1));
                }
                else if (workHandler.getClass() == MultiSaverWrapper.class)
                {
                    thread.setName("数据同步-多线程保存-" + count.getAndAdd(1));
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
