package org.jauntsy.grinder.karma.mapred;

/**
 * User: ebishop
 * Date: 1/23/13
 * Time: 4:49 PM
 */
public class Benchmark {
    private String name;
    private long totalTime;
    private long numberOfTimes;
    private long lastReport = System.currentTimeMillis();

    public Benchmark(String name) {
        this.name = name;
    }

    public Start start() {
        return new Start(System.currentTimeMillis());
    }

    public class Start {

        final long start;

        public Start(long start) {
            this.start = start;
        }

        public void end() {
            long now = System.currentTimeMillis();
            totalTime += now - start;
            numberOfTimes++;
            if (totalTime > 0 && now > lastReport + 1000) {
                long ops = 1000L * numberOfTimes / totalTime;
                long ms = ops == 0 ? -1 : (1000L / ops);
                System.out.println("#BENCH " + name + " calls: " + numberOfTimes + ", op/s: " + ops + ", ms: " + ms);
                lastReport = now;
            }
        }
    }
}
