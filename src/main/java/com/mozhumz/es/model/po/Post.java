package com.mozhumz.es.model.po;


import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
@Data
@Document(indexName="projectname",type="post",indexStoreType="fs",shards=5,replicas=1,refreshInterval="-1")
public class Post {
    @Id
    private String id;

    private String title;

    private String content;

    private int userId;

    private int weight;

}
