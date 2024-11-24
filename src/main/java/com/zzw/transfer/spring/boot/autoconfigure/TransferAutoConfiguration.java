package com.zzw.transfer.spring.boot.autoconfigure;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.zzw.transfer.spring.boot.transfer.*;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("all")
@AutoConfiguration
@EnableConfigurationProperties({TransferProperties.class})
public class TransferAutoConfiguration
{

    private static final int                SINGLE = 1;
    private final        TransferProperties properties;

    public TransferAutoConfiguration(TransferProperties properties)
    {
        this.properties = properties;
    }


    @Bean
    public Disruptor<Bucket> disruptor(List<Transfer> transferList)
    {
        Disruptor<Bucket> disruptor = getDisruptor();

        // 调度器
        Dispatcher dispatcher = new Dispatcher(transferList);

        // 生产者
        for (Transfer transfer : transferList)
        {
            transfer.setDisruptor(disruptor.getRingBuffer());
        }

        // 保存者
        MultiSaverWrapper   multiSaverWrapper  = new MultiSaverWrapper(dispatcher);
        MultiSaverWrapper[] multiSaverWrappers = new MultiSaverWrapper[properties.getSaverThreadNum()];
        Arrays.fill(multiSaverWrappers, multiSaverWrapper);

        // 消费者
        if (properties.getHandlerThreadNum() == SINGLE)
        {
            SingleHandlerWrapper singleHandlerWrapper = new SingleHandlerWrapper(dispatcher);
            disruptor.handleEventsWith(singleHandlerWrapper).thenHandleEventsWithWorkerPool(multiSaverWrappers);
        }
        else
        {
            MultiHandlerWrapper   multiHandlerWrapper  = new MultiHandlerWrapper(dispatcher);
            MultiHandlerWrapper[] multiHandlerWrappers = new MultiHandlerWrapper[properties.getHandlerThreadNum()];
            Arrays.fill(multiHandlerWrappers, multiHandlerWrapper);
            disruptor.handleEventsWithWorkerPool(multiHandlerWrappers).thenHandleEventsWithWorkerPool(multiSaverWrappers);
        }

        // 启动
        disruptor.start();

        return disruptor;
    }

    private Disruptor<Bucket> getDisruptor()
    {
        ThreadFactory threadFactory = new ThreadFactory()
        {
            final AtomicInteger count = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r)
            {
                Thread thread = new Thread(r);
                thread.setName("数据同步-消费线程-" + count.getAndAdd(1));
                return thread;
            }
        };

        // 512 * 2 k = 102 w
        // 512 * 5 k = 256 w
        // 512 * 1 w = 512 w
        return new Disruptor<>(
                Bucket::new,
                properties.getBufferSize(),
                threadFactory,
                properties.isMultiThreadWrite() ? ProducerType.MULTI : ProducerType.SINGLE,
                new BlockingWaitStrategy()
        );
    }
}
