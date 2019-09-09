package com.mozhumz.es.mapper;

import com.mozhumz.es.model.po.GoodsInfoPO;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Component;

@Component
public interface GoodsRepository extends ElasticsearchRepository<GoodsInfoPO,Long> {
}
