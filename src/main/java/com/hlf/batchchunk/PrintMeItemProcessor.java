package com.hlf.batchchunk;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;

@Slf4j
public class PrintMeItemProcessor<I> implements ItemProcessor<I, I> {

    private final String message;

    public PrintMeItemProcessor(String message) {
        this.message = message;
    }

    @Override
    public I process(I item) throws Exception {
        log.warn("PrintMe: " + message);
        return item;
    }
}
