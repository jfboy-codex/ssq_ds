package org.apache.dolphinscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

@Data
@TableName("ssq_host_thread")
public class HostThreadStatus implements Serializable {

    @TableId(value = "id", type = IdType.AUTO)
    private Integer id;

    private String host;

    private Integer threadId;

    private Date usableTime;

    private Long usableTimeStamp;

}
