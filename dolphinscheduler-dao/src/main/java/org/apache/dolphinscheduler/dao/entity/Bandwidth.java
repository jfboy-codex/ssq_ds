package org.apache.dolphinscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;

@Data
@TableName("ssq_bandwidth")
public class Bandwidth implements Serializable {

    @TableId(value = "id", type = IdType.AUTO)
    private Integer id;

    private String preHost;

    private String postHost;

    private Double bandwidth;

}
