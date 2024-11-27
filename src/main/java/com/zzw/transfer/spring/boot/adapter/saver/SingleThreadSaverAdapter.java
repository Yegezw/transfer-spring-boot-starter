package com.zzw.transfer.spring.boot.adapter.saver;

import com.lmax.disruptor.EventHandler;
import com.zzw.transfer.spring.boot.transfer.Bucket;
import com.zzw.transfer.spring.boot.transfer.Transfer;
import com.zzw.transfer.spring.boot.transfer.TransferRepository;

/**
 * 单线程保存器
 */
@SuppressWarnings("all")
public class SingleThreadSaverAdapter implements EventHandler<Bucket>
{

    private final TransferRepository transferRepository;

    public SingleThreadSaverAdapter(TransferRepository transferRepository)
    {
        this.transferRepository = transferRepository;
    }

    @Override
    public void onEvent(Bucket bucket, long sequence, boolean endOfBatch) throws Exception
    {
        Object   mark     = bucket.getMark();
        Transfer transfer = transferRepository.get(mark);
        transfer.save(bucket);
    }
}
