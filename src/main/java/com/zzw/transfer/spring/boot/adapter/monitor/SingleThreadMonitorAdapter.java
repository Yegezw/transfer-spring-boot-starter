package com.zzw.transfer.spring.boot.adapter.monitor;

import com.lmax.disruptor.EventHandler;
import com.zzw.transfer.spring.boot.transfer.Bucket;
import com.zzw.transfer.spring.boot.transfer.TransferRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 单线程监控器
 */
public class SingleThreadMonitorAdapter implements EventHandler<Bucket>
{

    private static final Logger log = LoggerFactory.getLogger(SingleThreadMonitorAdapter.class);

    private final TransferRepository transferRepository;

    public SingleThreadMonitorAdapter(TransferRepository transferRepository)
    {
        this.transferRepository = transferRepository;
    }

    @Override
    public void onEvent(Bucket bucket, long sequence, boolean endOfBatch)
    {
        transferRepository.updateCount(bucket);
        if (bucket.isLastPublish())
        {
            double time = transferRepository.stop(bucket);
            if (log.isInfoEnabled())
            {
                log.info("{} 累计 {} 条数据同步完成, 耗时 {} s", bucket.getMark(), transferRepository.getCount(bucket), time);
            }
            transferRepository.resetCount(bucket);
        }
        bucket.clear(); // help gc
    }
}
