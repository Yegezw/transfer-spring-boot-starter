package com.zzw.transfer.spring.boot.transfer;

import com.lmax.disruptor.EventHandler;

/**
 * 单线程消费者
 */
@SuppressWarnings("all")
public class SingleHandlerWrapper implements EventHandler<Bucket>
{

    private final Dispatcher dispatcher;

    public SingleHandlerWrapper(Dispatcher dispatcher)
    {
        this.dispatcher = dispatcher;
    }

    @Override
    public void onEvent(Bucket bucket, long sequence, boolean endOfBatch) throws Exception
    {
        Object   mark     = bucket.getMark();
        Transfer transfer = dispatcher.getTransfer(mark);
        transfer.handle(bucket);
    }
}
