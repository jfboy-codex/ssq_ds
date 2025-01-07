package org.apache.dolphinscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.dolphinscheduler.dao.entity.Bandwidth;
import org.apache.dolphinscheduler.dao.entity.HostThreadStatus;

import java.util.List;

public interface HostThreadStatusMapper extends BaseMapper<HostThreadStatus> {

    List<HostThreadStatus> queryAll();

    void update(HostThreadStatus hostThreadStatus);
}
