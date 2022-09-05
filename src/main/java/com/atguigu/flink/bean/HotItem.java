package com.atguigu.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: dsy
 * @Date: 2022/8/8 21:21
 * @Desciption:
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class HotItem {
    private Long itemId;
    private Long wEnd;
    private Long count;
}
