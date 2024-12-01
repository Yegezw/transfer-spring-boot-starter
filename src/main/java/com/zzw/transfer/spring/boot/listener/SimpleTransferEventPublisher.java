package com.zzw.transfer.spring.boot.listener;

import java.util.ArrayList;
import java.util.List;

public class SimpleTransferEventPublisher
{

    private final List<TransferListener> listeners;

    public SimpleTransferEventPublisher()
    {
        this.listeners = new ArrayList<>();
    }

    public void publishEvent(TransferEvent event)
    {
        for (TransferListener listener : listeners)
        {
            listener.onTransferEvent(event);
        }
    }

    public void addTransferListener(TransferListener listener)
    {
        listeners.add(listener);
    }

    public void addTransferListener(List<TransferListener> listeners)
    {
        this.listeners.addAll(listeners);
    }
}
