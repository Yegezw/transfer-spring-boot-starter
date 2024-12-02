package com.zzw.transfer.spring.boot.autoconfigure;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import com.zzw.transfer.spring.boot.adapter.TransferThreadFactory;
import com.zzw.transfer.spring.boot.adapter.handler.MultiThreadHandlerAdapter;
import com.zzw.transfer.spring.boot.adapter.handler.SingleThreadHandlerAdapter;
import com.zzw.transfer.spring.boot.adapter.monitor.SingleThreadMonitorAdapter;
import com.zzw.transfer.spring.boot.adapter.saver.MultiThreadSaverAdapter;
import com.zzw.transfer.spring.boot.adapter.saver.SingleThreadSaverAdapter;
import com.zzw.transfer.spring.boot.listener.TransferListener;
import com.zzw.transfer.spring.boot.transfer.Bucket;
import com.zzw.transfer.spring.boot.transfer.Transfer;
import com.zzw.transfer.spring.boot.transfer.TransferChain;
import com.zzw.transfer.spring.boot.transfer.TransferRepository;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.Arrays;
import java.util.List;

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
    public TransferChain transferChain(List<Transfer<?, ?>> transferList)
    {
        return new TransferChain(transferList);
    }

    @Bean
    public Disruptor<Bucket> disruptor(List<Transfer<?, ?>> transferList, List<TransferListener> listeners)
    {
        Disruptor<Bucket>         disruptor = getDisruptor();
        EventHandlerGroup<Bucket> group;

        // 转移器仓库
        TransferRepository transferRepository = new TransferRepository(transferList, listeners);

        // 1、生产器
        for (Transfer<?, ?> transfer : transferList)
        {
            transfer.setDisruptor(disruptor.getRingBuffer());
            transfer.setTransferRepository(transferRepository);
        }

        // 2、处理器
        if (properties.getHandlerThreadNum() == SINGLE)
        {
            SingleThreadHandlerAdapter singleThreadHandlerAdapter = new SingleThreadHandlerAdapter(transferRepository);
            group = thenSingleThread(disruptor, singleThreadHandlerAdapter);
        }
        else
        {
            MultiThreadHandlerAdapter multiThreadHandlerAdapter = new MultiThreadHandlerAdapter(transferRepository);
            group = thenMultiThread(disruptor, multiThreadHandlerAdapter, properties.getHandlerThreadNum());
        }

        // 3、保存器
        if (properties.getSaverThreadNum() == SINGLE)
        {
            SingleThreadSaverAdapter singleThreadSaverAdapter = new SingleThreadSaverAdapter(transferRepository);
            group = thenSingleThread(group, singleThreadSaverAdapter);
        }
        else
        {
            MultiThreadSaverAdapter multiThreadSaverAdapter = new MultiThreadSaverAdapter(transferRepository);
            group = thenMultiThread(group, multiThreadSaverAdapter, properties.getSaverThreadNum());
        }

        // 4、监控器
        SingleThreadMonitorAdapter singleThreadMonitorAdapter = new SingleThreadMonitorAdapter(transferRepository);
        thenSingleThread(group, singleThreadMonitorAdapter);

        // 启动
        disruptor.start();

        return disruptor;
    }

    private Disruptor<Bucket> getDisruptor()
    {
        // 512 * 2 k = 102 w
        // 512 * 5 k = 256 w
        // 512 * 1 w = 512 w
        return new Disruptor<>(
                Bucket::new,
                properties.getBufferSize(),
                new TransferThreadFactory(),
                properties.isMultiThreadWrite() ? ProducerType.MULTI : ProducerType.SINGLE,
                new BlockingWaitStrategy()
        );
    }


    private static EventHandlerGroup<Bucket> thenSingleThread(Disruptor<Bucket> disruptor, EventHandler<Bucket> eventHandler)
    {
        return disruptor.handleEventsWith(eventHandler);
    }

    @SuppressWarnings("unchecked")
    private static EventHandlerGroup<Bucket> thenMultiThread(Disruptor<Bucket> disruptor, WorkHandler<Bucket> workHandler, int threadNum)
    {
        WorkHandler<Bucket>[] workHandlers = new WorkHandler[threadNum];
        Arrays.fill(workHandlers, workHandler);
        return disruptor.handleEventsWithWorkerPool(workHandlers);
    }

    private static EventHandlerGroup<Bucket> thenSingleThread(EventHandlerGroup<Bucket> group, EventHandler<Bucket> eventHandler)
    {
        return group.handleEventsWith(eventHandler);
    }

    @SuppressWarnings("unchecked")
    private static EventHandlerGroup<Bucket> thenMultiThread(EventHandlerGroup<Bucket> group, WorkHandler<Bucket> workHandler, int threadNum)
    {
        WorkHandler<Bucket>[] workHandlers = new WorkHandler[threadNum];
        Arrays.fill(workHandlers, workHandler);
        return group.handleEventsWithWorkerPool(workHandlers);
    }
}
