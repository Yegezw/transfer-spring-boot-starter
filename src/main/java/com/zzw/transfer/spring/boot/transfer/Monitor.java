package com.zzw.transfer.spring.boot.transfer;

import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Monitor implements EventHandler<Bucket>
{

    private static final Logger log = LoggerFactory.getLogger(Monitor.class);

    private final Dispatcher dispatcher;

    public Monitor(Dispatcher dispatcher)
    {
        this.dispatcher = dispatcher;
    }

    @Override
    public void onEvent(Bucket bucket, long sequence, boolean endOfBatch)
    {
        dispatcher.updateCount(bucket);
        if (bucket.isLastPublish())
        {
            double time = dispatcher.stop(bucket);
            if (log.isInfoEnabled())
            {
                log.info("{} 累计 {} 条数据同步完成, 耗时 {} s", bucket.getMark(), dispatcher.getCount(bucket), time);
            }
            dispatcher.resetCount(bucket);
        }
        bucket.clear(); // help gc
    }
}
