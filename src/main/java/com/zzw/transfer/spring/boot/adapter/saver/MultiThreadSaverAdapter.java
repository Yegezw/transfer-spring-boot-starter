package com.zzw.transfer.spring.boot.adapter.saver;

import com.lmax.disruptor.WorkHandler;
import com.zzw.transfer.spring.boot.transfer.Bucket;
import com.zzw.transfer.spring.boot.transfer.TransferRepository;
import com.zzw.transfer.spring.boot.transfer.Transfer;

/**
 * 多线程保存器
 */
public class MultiThreadSaverAdapter implements WorkHandler<Bucket>
{

    private final TransferRepository transferRepository;

    public MultiThreadSaverAdapter(TransferRepository transferRepository)
    {
        this.transferRepository = transferRepository;
    }

    @Override
    public void onEvent(Bucket bucket)
    {
        Object         mark     = bucket.getMark();
        Transfer<?, ?> transfer = transferRepository.get(mark);
        transfer.save(bucket);
    }
}
