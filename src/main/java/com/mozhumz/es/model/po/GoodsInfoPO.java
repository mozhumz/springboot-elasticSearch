package com.mozhumz.es.model.po;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.elasticsearch.annotations.Document;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Document(indexName = "testgoods",type = "goods")
//indexName索引名称 可以理解为数据库名 必须为小写 不然会报org.elasticsearch.indices.InvalidIndexNameException异常
//type类型 可以理解为表名
public class GoodsInfoPO implements Serializable {
    private Long id;
    private String name;
    private String description;
}
