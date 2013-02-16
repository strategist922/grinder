package org.jauntsy.nice;

import backtype.storm.utils.Utils;

import java.util.Scanner;

/**
 * User: ebishop
 * Date: 1/8/13
 * Time: 10:52 AM
 */
public class Demo {

    public static void countdown(String label, int seconds) {
        countdown(1000, label, seconds);
    }

    public static void countdown2(String label, int seconds) {
        countdown2(1000, label, seconds);
    }

    public static void countdown(long msDelayBeforeMessage, String label, int seconds) {
        Utils.sleep(msDelayBeforeMessage);
        System.out.println();
        while (seconds > 0) {
            System.out.println(label + " " + seconds);
            Utils.sleep(1000);
            seconds--;
        }
    }

    public static void countdown2(long delayBeforeMessage, String label, int seconds) {
        Utils.sleep(delayBeforeMessage);
        System.out.println();
        System.out.print(label + ": " + seconds);
        System.out.flush();
        while (seconds > 0) {
            Utils.sleep(1000);
            seconds--;
            if (seconds > 0) {
                System.out.print(" " + seconds);
                System.out.flush();
            }
        }
        System.out.println();
    }

    public static void readEnter(String msg, int delay) {
        Utils.sleep(1000 * delay);
        System.out.print("\n" + msg + ": Hit enter to continue...");
        System.out.flush();
        Scanner sc = new Scanner(System.in);
        while(!sc.nextLine().equals(""));
    }

}
