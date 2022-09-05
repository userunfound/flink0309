package com.atguigu.flink.util;

import com.atguigu.flink.bean.WaterSensor;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @author:dsy
 * @date:2022/8/4 20:33
 * @desciption:
 */


public class AtguiguUtil {

    public static String toDateTime(long ts) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ts);

    }

    public static <T> List<T> toList(Iterable<T> it) {
        List<T> list = new ArrayList<>();

        for (T t : it) {
            list.add(t);
        }
        return list;

    }

    public static String toDate(Long ts) {
        return new SimpleDateFormat("yyyy-MM-dd").format(ts);
    }
}
