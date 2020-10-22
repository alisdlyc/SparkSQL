package com.alisdlyc.sparksql;

public class qwq {
    public static void main(String[] args) {
        new Runnable() {
            @Override
            public void run() {
                System.out.println("qwq");
            }
        }.run();

        new Thread(new MyThread()).run();
        new Thread(new InterfaceThread()).run();
    }
}

class MyThread extends Thread {

    @Override
    public void run() {
        System.out.println("MyThread");
    }
}

class InterfaceThread implements Runnable {

    @Override
    public void run() {
        System.out.println("Runnable");
    }
}