package com.atguigu.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: dsy
 * @Date: 2022/8/13 16:20
 * @Desciption:
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AdsClickLog {
    private Long userId;
    private Long adsId;
    private String province;
    private String city;
    private Long timestamp;
}
