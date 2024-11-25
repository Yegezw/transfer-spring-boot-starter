package com.zzw.transfer.spring.boot.transfer;

import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SaverMonitor implements EventHandler<Bucket>
{

    private static final Logger log = LoggerFactory.getLogger(SaverMonitor.class);

    private final Dispatcher dispatcher;

    public SaverMonitor(Dispatcher dispatcher)
    {
        this.dispatcher = dispatcher;
    }

    @Override
    public void onEvent(Bucket bucket, long sequence, boolean endOfBatch)
    {
        dispatcher.updateCount(bucket);
        if (bucket.isLastPublish())
        {
            if (log.isInfoEnabled())
            {
                log.info("{} 累计 {} 条数据同步完成", bucket.getMark(), dispatcher.getCount(bucket));
            }
            dispatcher.resetCount(bucket);
        }
        bucket.clear(); // help gc
    }
}
