package com.zzw.transfer.spring.boot.listener;

import java.util.EventListener;

public interface TransferListener extends EventListener
{

    void onTransferEvent(TransferEvent event);
}
