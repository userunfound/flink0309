package com.atguigu.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author:dsy
 * @date:2022/8/4 19:46
 * @desciption:
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TxEvent {
    private String txId;
    private String payChannel;
    private Long eventTime;
}
