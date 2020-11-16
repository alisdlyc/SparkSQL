package com.alisdlyc.JavaJVM;

public class Car {
    public static void main(String[] args) {
        Car car1 = new Car();
        Car car2 = new Car();
        Car car3 = new Car();

        Class<? extends Car> aClass1 = car1.getClass();
        Class<? extends Car> aClass2 = car1.getClass();
        Class<? extends Car> aClass3 = car1.getClass();

        System.out.println(aClass1.hashCode());
        System.out.println(aClass2.hashCode());
        System.out.println(aClass3.hashCode());

        ClassLoader classLoader = aClass1.getClassLoader();
        System.out.println(classLoader); // AppClassLoader
        System.out.println(classLoader.getParent());// PlatformClassLoader
        System.out.println(classLoader.getParent().getParent());//null rt.jar
    }
}
