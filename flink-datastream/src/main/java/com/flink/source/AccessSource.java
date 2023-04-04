package com.flink.source;

import com.flink.bean.Access;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class AccessSource implements SourceFunction<Access> {

    boolean isRunning = true;

    @Override
    public void run(SourceContext<Access> sourceContext) throws Exception {

        Random random = new Random();
        String[] domains = {"hmx1.com","hmx2.com","hmx3.com"};
        while (isRunning==true){
            long time = System.currentTimeMillis();
            sourceContext.collect(new Access(time,domains[random.nextInt(domains.length)],random.nextInt(1000)+1000));
            Thread.sleep(200);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
