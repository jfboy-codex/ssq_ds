package org.apache.dolphinscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.dolphinscheduler.dao.entity.Bandwidth;

import java.util.List;

public interface DataCenterBandwidthMapper extends BaseMapper<Bandwidth> {

    List<Bandwidth> queryAll();

}
