package com.mozhumz.es.mapper;

import com.mozhumz.es.model.po.Post;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

public interface PostRepository extends ElasticsearchRepository<Post, String> {
}
