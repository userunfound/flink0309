package com.atguigu.flink.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @desciption:
 * @author:
 * @date:
 */


@Data
@NoArgsConstructor
@AllArgsConstructor

public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;
}
