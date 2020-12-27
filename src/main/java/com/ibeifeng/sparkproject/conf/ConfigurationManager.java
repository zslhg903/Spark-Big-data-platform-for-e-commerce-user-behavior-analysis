package com.ibeifeng.sparkproject.conf;

import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 *
 * 1.配置管理组件 可以复杂 ，也可以简单，对于简单的配置管理组件 ，只要开发一个类，可以在第一次访问它时，就从对应的properties文件中，读取配置项，并提供外界获取 某个蕾key对应的value方法
 * 2.如果是特别复杂 的配置管理组件 ，那么可能 需要使用一些软件设计中的设计模式，比如单例，解释器模式，可能 需要管理多个不同的properties，甚至是xml类型的配置文件
 * 3.我们这里使用简单的
*/
public class ConfigurationManager {

    private static Properties prop=new Properties();

    /**
     * 静态代码块
     * Java中，每一个类第一次使用的时候，就会被Java虚拟机（JVM）中的类加载器，去从磁盘上的.class文件中
     * 加载出来 ，然后为每个类都会构建 一个Class对象，就代表了这个类
     *
     * 每个类 在第一次加载的时候，都会进行自身的初始化，类初始化时，由每个类内部的static()构成的静态代码块决定操作
     * 类第一次使用的时候，就会加载，加载时初始化类，初始化类的时候就会执行类的静态代码块
     *
     * 因此 ，对于我们的配置管理组件 ，就在静态代码块中，编写读取配置文件的代码
     *
     * 而且，放在静态代码块中，还有一个好处，就是类的初始化在整 个JVM生命周期内，有且仅有一次，即
     * 配置文件只会加载一次，然后以后就会重复使用，效率比较高，不用反复加载。
     */
    static {
        try {
            //通过一个类名.class的方式，就可以获取 到这个类在JVM中对应的Class对象
            //然后再通过这个Class对象的getClassLoader()方法，就可以获取 到当初加载这个类的JVM中的类加载器，
            //然后调用ClassLoader的getResourceAsStream()方法
            //最终可以获取 到一个针对 指定文件的输入流(InputStream)
            InputStream in=ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");

            prop.load(in);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static String getProperty(String key){
        return prop.getProperty(key);
    }

    /**
     * 获取整数类型的配置项
     * @param key
     * @return value
     */
    public static Integer getInteger(String key) {
        String value = getProperty(key);
        try {
            return Integer.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取布尔类型的配置项
     * @param key
     * @return value
     */
    public static Boolean getBoolean(String key) {
        String value = getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 获取Long类型的配置项
     * @param key
     * @return
     */
    public static Long getLong(String key) {
        String value = getProperty(key);
        try {
            return Long.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }
}
