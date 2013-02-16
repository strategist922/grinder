package org.jauntsy.nice;

/**
 * User: ebishop
 * Date: 12/11/12
 * Time: 9:25 AM
 */
public class Time {

    public static long now() {
        return System.currentTimeMillis();
    }

    public static long since(long then) {
        return now() - then;
    }

}
