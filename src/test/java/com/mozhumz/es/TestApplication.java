package com.mozhumz.es;

import com.mozhumz.es.mapper.GoodsRepository;
import com.mozhumz.es.mapper.PostRepository;
import com.mozhumz.es.model.po.GoodsInfoPO;
import com.mozhumz.es.model.po.Post;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;

@RunWith(SpringRunner.class)
@SpringBootTest
public class TestApplication {
    @Autowired
    private GoodsRepository goodsRepository;
    @Autowired
    private TransportClient client;
    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;
    @Autowired
    private PostRepository postRepository;

    //elasticsearchTemplate使用

    /**
     * 单字符串模糊查询，默认排序。将从所有字段中查找包含传来的word分词后字符串的数据集
     */
    @Test
    public void singleTitle( ) {
        //使用queryStringQuery完成单字符串查询
        String word="浣溪沙";
        Pageable pageable= PageRequest.of(0,20);
        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(QueryBuilders.queryStringQuery(word)).withPageable(pageable).build();
        List<Post> list= elasticsearchTemplate.queryForList(searchQuery, Post.class);
        System.out.println(list.size());
        list.forEach(item-> System.out.println(item));
    }

    /**
     * 单字符串模糊查询，单字段排序。
     */
    @Test
    public void singlePost() {
        //使用queryStringQuery完成单字符串查询
        String word="浣溪沙";
        Pageable pageable= PageRequest.of(0,20, new Sort(Sort.Direction.DESC,"weight"));


        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(queryStringQuery(word)).withPageable(pageable).build();
        List<Post> list= elasticsearchTemplate.queryForList(searchQuery, Post.class);
        System.out.println(list.size());
        list.forEach(item-> System.out.println(item));
    }

    /**
     * 单字段对某字符串模糊查询
     */
    @Test
    public void singleMatch() {
        String word="落日熔金";
        Pageable pageable= PageRequest.of(0,20);
        //单字段-模糊查询
//        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(QueryBuilders.matchQuery("content", word))
//                .withPageable(pageable).build();
        /***
         *多字段匹配-模糊查询  Type可以更改算分方式，不影响查询结果：
         * BEST_FIELDS：我们希望完全匹配的文档评分比较高
         * MOST_FIELDS：我们希望多字段匹配的文档评分比较高
         * CROSS_FIELDS；我们希望这个词条的分词是分配到不同字段中的
         */

//        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(QueryBuilders.multiMatchQuery(word, "content","title")
//                .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS))
//                .withPageable(pageable).build();
        //短语匹配
//        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(QueryBuilders.matchPhraseQuery("content", word))
//                .withPageable(pageable).build();
        //fuzzy 模糊匹配
//        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(QueryBuilders.fuzzyQuery("content", word))
//                .withPageable(pageable).build();
        word="中华人民共和国";
        /**
         *slop参数告诉match_phrase查询词条能够相隔多远时仍然将文档视为匹配
         * 尽管在使用了slop的短语匹配中，所有的单词都需要出现，但是单词的出现顺序可以不同。如果slop的值足够大，那么单词的顺序可以是任意的
         */

//        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(QueryBuilders.matchPhraseQuery("content", word).slop(6))
//                .withPageable(pageable).build();

        /**
         * 之前的查询中，当我们输入“我天”时，ES会把分词后所有包含“我”和“天”的都查询出来，
         * 如果我们希望必须是包含了两个字的才能被查询出来，那么我们就需要设置一下Operator
         */
//        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(QueryBuilders.matchQuery("content", word).operator(
//                Operator.AND
//        ))
//                .withPageable(pageable).build();

        /**
         * 无论是matchQuery，multiMatchQuery，queryStringQuery等，都可以设置operator。默认为Or，设置为And后，就会把符合包含所有输入的才查出来。
         * 如果是and的话，譬如用户输入了5个词，但包含了4个，也是显示不出来的。我们可以通过设置精度来控制
         */

        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(QueryBuilders.matchQuery("content", word).operator(
                Operator.AND
        )).withMinScore(0.6F)
                .withPageable(pageable).build();
//        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(QueryBuilders.matchQuery("userId", userId)).withPageable(pageable).build();
        List<Post> list= elasticsearchTemplate.queryForList(searchQuery, Post.class);
        System.out.println(list.size());
        list.forEach(item-> System.out.println(item));
    }

    @Test
    public void add() {
        Post post = new Post();
        post.setTitle("我是");
        post.setContent("我爱中华人民共和国");
        post.setWeight(1);
        post.setUserId(1);
        postRepository.save(post);
        post = new Post();
        post.setTitle("我是");
        post.setContent("中华共和国");
        post.setWeight(2);
        post.setUserId(2);
        postRepository.save(post);
    }


    //客户端-查询
    @Test
    public void find(){
        GetResponse getResponse=client.prepareGet("lib3","user","1").execute().actionGet();
        System.out.println(getResponse.getSourceAsString());
    }

    //客户端添加文档
    @Test
    public void CAdd() throws IOException {
        XContentBuilder doc= XContentFactory.jsonBuilder().startObject()
                .field("id","1")
                .field("title","Java设计模式之装饰模式")
                .field("content","动态扩展一个对象")
                .field("postdate","2018-05-20")
                .field("url","csdn.net/79239072")
                .endObject();

        IndexResponse indexResponse=client.prepareIndex("testblog","blog","10").setSource(doc).get();

        System.out.println(indexResponse.status());
    }
    //客户端-更新文档
    @Test
    public void cUpdate() throws ExecutionException, InterruptedException, IOException {
        UpdateRequest updateRequest=new UpdateRequest();
        updateRequest.index("testblog").type("blog").id("10").doc(XContentFactory.jsonBuilder().startObject()
        .field("title","单例设计模式").endObject());
        UpdateResponse response=client.update(updateRequest).get();
        System.out.println(response.status());
    }
    //客户端-upsert
    @Test
    public void cUpsert() throws IOException, ExecutionException, InterruptedException {
        IndexRequest indexRequest=new IndexRequest("testblog","blog","8")
                .source(
                        XContentFactory.jsonBuilder().startObject()
                                .field("id","2")
                                .field("title","工厂模式")
                                .field("content","静态工厂，动态工厂")
                                .field("postdate","2018-05-20")
                                .field("url","csdn.net/79239072")
                                .endObject()
                );
        UpdateRequest updateRequest=new UpdateRequest("testblog","blog","8").doc(
                XContentFactory.jsonBuilder().startObject()
                        .field("title","设计模式").endObject()
        ).upsert(indexRequest);
        UpdateResponse updateResponse=client.update(updateRequest).get();
        System.out.println(updateResponse.status());
    }

    //客户端-删除
    @Test
    public void CDel(){
        DeleteResponse deleteResponse=client.prepareDelete("testblog","blog","10").get();
        System.out.println(deleteResponse.status());
    }

    //客户端-批量查询
    @Test
    public void cMultiget(){
        MultiGetResponse multiGetItemResponses=client.prepareMultiGet()
                .add("testblog","blog","8","10")
                .add("lib3","user","1","2","3").get();
        for(MultiGetItemResponse itemResponse:multiGetItemResponses){
            GetResponse getResponse=itemResponse.getResponse();
            if(getResponse!=null&&getResponse.isExists()){
                System.out.println(getResponse.getSourceAsString());
            }
        }
    }

    //客户端-批量新增
    @Test
    public void cBulkAdd() throws IOException {
        BulkRequestBuilder bulkRequestBuilder=client.prepareBulk();
        bulkRequestBuilder.add(client.prepareIndex("lib2","books","8")
        .setSource(XContentFactory.jsonBuilder().startObject()
        .field("title","python")
        .field("price",99).endObject()));

        bulkRequestBuilder.add(client.prepareIndex("lib2","books","9")
                .setSource(XContentFactory.jsonBuilder().startObject()
                        .field("title","VR")
                        .field("price",29).endObject()));

        BulkResponse bulkItemResponses=bulkRequestBuilder.get();
        System.out.println(bulkItemResponses.status());


    }

    //客户端-查询删除
    @Test
    public void cFindDel(){
        BulkByScrollResponse response= DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery(
                "title","单例"
        )).source("testblog").get();

        long s=response.getDeleted();
        System.out.println(s);

    }

    //客户端-matchAllQuery
    @Test
    public void cQuery(){
//        QueryBuilder queryBuilder=QueryBuilders.matchAllQuery();
        QueryBuilder queryBuilder=QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery());
        SearchResponse searchResponse=client.prepareSearch("lib3").setQuery(queryBuilder).setSize(3).get();
        SearchHits searchHits=searchResponse.getHits();
        for(SearchHit searchHit:searchHits){
            System.out.println(searchHit.getSourceAsString());
        }

    }

    //客户端-matchQuery
    @Test
    public void matchQuery(){
        QueryBuilder queryBuilder=QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("interests","changge"))
                .should(QueryBuilders.matchQuery("interests","lvyou"));
        SearchResponse searchResponse=client.prepareSearch("lib3").setQuery(queryBuilder).setSize(10).get();
        SearchHits searchHits=searchResponse.getHits();
        for(SearchHit searchHit:searchHits){
            System.out.println(searchHit.getSourceAsString());
        }
    }

    //客户端-multiMatchQuery
    @Test
    public void multiMatchQuery(){
        QueryBuilder queryBuilder=QueryBuilders.multiMatchQuery("changge,lvyou","interests","address");
        SearchResponse searchResponse=client.prepareSearch("lib3").setQuery(queryBuilder).setSize(10).get();
        SearchHits searchHits=searchResponse.getHits();
        for(SearchHit searchHit:searchHits){
            System.out.println(searchHit.getSourceAsString());
        }
    }

    //客户端-termQuery
    @Test
    public void termQuery(){
        QueryBuilder queryBuilder=QueryBuilders.termQuery("changge,lvyou","interests");
        SearchResponse searchResponse=client.prepareSearch("lib3").setQuery(queryBuilder).setSize(10).get();
        SearchHits searchHits=searchResponse.getHits();
        for(SearchHit searchHit:searchHits){
            System.out.println(searchHit.getSourceAsString());
        }
    }

    //客户端-termsQuery
    @Test
    public void termsQuery(){
        QueryBuilder queryBuilder=QueryBuilders.termsQuery("interests","changge","lvyou");
        SearchResponse searchResponse=client.prepareSearch("lib3").setQuery(queryBuilder).setSize(10).get();
        SearchHits searchHits=searchResponse.getHits();
        for(SearchHit searchHit:searchHits){
            System.out.println(searchHit.getSourceAsString());
        }
    }


    //客户端-rangeQuery
    @Test
    public void rangeQuery(){
//        QueryBuilder queryBuilder=QueryBuilders.rangeQuery("birthday").from("1990-01-01").to("2000-01-01")
//                .format("yyyy-MM-dd");
        //前缀查询
//        QueryBuilder queryBuilder=QueryBuilders.prefixQuery("name","zhao");
        //通配符wildcard查询
//        QueryBuilder queryBuilder=QueryBuilders.wildcardQuery("name","zhao*");
        //fuzzy查询
//        QueryBuilder queryBuilder=QueryBuilders.fuzzyQuery("interests","chagge");
        //ids查询
        String ids[]={"1","2","3"};
        QueryBuilder queryBuilder=QueryBuilders.idsQuery().addIds(ids);

        SearchResponse searchResponse=client.prepareSearch("lib3").setQuery(queryBuilder).setSize(10).get();
        SearchHits searchHits=searchResponse.getHits();
        for(SearchHit searchHit:searchHits){
            System.out.println(searchHit.getSourceAsString());
        }
    }

    //客户端-聚合查询
    @Test
    public void addAggregation(){
        //最大值 最小值 平均值 总和
//        AggregationBuilder aggregationBuilder= AggregationBuilders.max("ageMax").field("age");
//        SearchResponse searchResponse=client.prepareSearch("lib3").addAggregation(aggregationBuilder).get();
//        Max max=searchResponse.getAggregations().get("ageMax");
//        System.out.println(max.getValue());

//        AggregationBuilder aggregationBuilder= AggregationBuilders.min("ageMin").field("age");
//        SearchResponse searchResponse=client.prepareSearch("lib3").addAggregation(aggregationBuilder).get();
//        Min min=searchResponse.getAggregations().get("ageMin");
//        System.out.println(min.getValue());

//        AggregationBuilder aggregationBuilder= AggregationBuilders.avg("ageAvg").field("age");
//        SearchResponse searchResponse=client.prepareSearch("lib3").addAggregation(aggregationBuilder).get();
//        Avg min=searchResponse.getAggregations().get("ageAvg");
//        System.out.println(min.getValue());

//        AggregationBuilder aggregationBuilder= AggregationBuilders.sum("ageAvg").field("age");
//        SearchResponse searchResponse=client.prepareSearch("lib3").addAggregation(aggregationBuilder).get();
//        Sum min=searchResponse.getAggregations().get("ageAvg");
//        System.out.println(min.getValue());

        //去重计算个数
//        AggregationBuilder aggregationBuilder= AggregationBuilders.cardinality("ageAvg").field("age");
//        SearchResponse searchResponse=client.prepareSearch("lib3").addAggregation(aggregationBuilder).get();
//        Cardinality min=searchResponse.getAggregations().get("ageAvg");
//        System.out.println(min.getValue());

        //分组统计
//        AggregationBuilder aggregationBuilder= AggregationBuilders.terms("ageAvg").field("age");
//        SearchResponse searchResponse=client.prepareSearch("lib3").addAggregation(aggregationBuilder).execute()
//                .actionGet();
//        Terms terms=searchResponse.getAggregations().get("ageAvg");
//        for(Terms.Bucket bucket:terms.getBuckets()){
//            System.out.println(bucket.getKey()+","+bucket.getDocCount());
//        }

        //单个过滤器
//        QueryBuilder queryBuilder=QueryBuilders.termQuery("age",20);
//        AggregationBuilder aggregationBuilder= AggregationBuilders.filter("ageAvg",queryBuilder);
//        SearchResponse searchResponse=client.prepareSearch("lib3").addAggregation(aggregationBuilder).execute()
//                .actionGet();
//        Filter filter=searchResponse.getAggregations().get("ageAvg");
//        System.out.println(filter.getDocCount());

//多过滤器
//        AggregationBuilder aggregationBuilder= AggregationBuilders.filters("ageAvg",
//                new FiltersAggregator.KeyedFilter("changge",QueryBuilders.termQuery("interests","changge")),
//                new FiltersAggregator.KeyedFilter("hejiu",QueryBuilders.termQuery("interests","hejiu"))
//
//        );
//        SearchResponse searchResponse=client.prepareSearch("lib3").addAggregation(aggregationBuilder).execute()
//                .actionGet();
//        Filters terms=searchResponse.getAggregations().get("ageAvg");
//        for(Filters.Bucket bucket:terms.getBuckets()){
//            System.out.println(bucket.getKey()+","+bucket.getDocCount());
//        }

        //范围查询 (小于50 [26-50) 和大于等于26的值)
        AggregationBuilder aggregationBuilder= AggregationBuilders.range("range").field("age").addUnboundedFrom(26).addUnboundedTo(50)
                .addRange(26,50);
        SearchResponse searchResponse=client.prepareSearch("lib3").addAggregation(aggregationBuilder).execute().actionGet();
        Range range=searchResponse.getAggregations().get("range");
        for(Range.Bucket bucket:range.getBuckets()){
            System.out.println(bucket.getKey()+","+bucket.getDocCount());
        }

        //查询price为空的记录
//        AggregationBuilder aggregationBuilder= AggregationBuilders.missing("range").field("price");
//        SearchResponse searchResponse=client.prepareSearch("article").addAggregation(aggregationBuilder).execute().actionGet();
//        Aggregation range=searchResponse.getAggregations().get("range");
//        System.out.println(range.toString());
    }

    //客户端-commonTermsQuery
    @Test
    public void commonTermsQuery(){
//        即GET /lib3/user/_search?q=name:zhaoliu,wangwu
//        QueryBuilder queryBuilder=QueryBuilders.commonTermsQuery("name","zhaoliu,wangwu");

        //全文检索 全部条件必须都满足 +表示必须有的条件 -表示必须没有的条件
//        QueryBuilder queryBuilder=QueryBuilders.queryStringQuery("+changge -hejiu");
        //全文检索 满足之一即可
//        QueryBuilder queryBuilder=QueryBuilders.simpleQueryStringQuery("+changge -hejiu");
//不计分查询
        QueryBuilder queryBuilder=QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("name","zhaoliu"));
        SearchResponse searchResponse=client.prepareSearch("lib3").setQuery(queryBuilder).setSize(10).get();
        SearchHits searchHits=searchResponse.getHits();
        for(SearchHit searchHit:searchHits){
            System.out.println(searchHit.getSourceAsString());
        }
    }

    /**
     * 集群健康状态
     */
    @Test
    public void health(){
        ClusterHealthResponse response=client.admin().cluster().prepareHealth().get();
        System.out.println(response.getClusterName());

        System.out.println(response.getNumberOfDataNodes());

        System.out.println(response.getNumberOfNodes());

        for(ClusterIndexHealth clusterIndexHealth:response.getIndices().values()){
            int shards=clusterIndexHealth.getNumberOfShards();
            int replicas=clusterIndexHealth.getNumberOfReplicas();
            System.out.println("shards&replicas:"+shards+","+replicas);
            ClusterHealthStatus status=clusterIndexHealth.getStatus();
            System.out.println(status);
        }
    }

    @Test
    public void testSave(){
        System.out.println("save start...");
        GoodsInfoPO goodsInfo = new GoodsInfoPO(System.currentTimeMillis(),
                "商品"+System.currentTimeMillis(),"这是一个测试商品");
        goodsRepository.save(goodsInfo);

        System.out.println(goodsInfo);

        System.out.println("save ok");
    }

    @Test
    public void testDelete(){
        System.out.println("delete start...");
        long id=1;
        goodsRepository.deleteById(id);

        System.out.println("delete ok");
    }

    @Test
    public void testUpdate(){
        System.out.println("update start...");
        long id=1;
        String name="";
        String description="";
        GoodsInfoPO goodsInfo = new GoodsInfoPO(id,
                name,description);
        goodsRepository.save(goodsInfo);


        System.out.println("update ok");
    }

    @Test
    public void testFind(){
        long id=1;
        GoodsInfoPO goodsInfoPO=goodsRepository.findById(id).get();
        System.out.println(goodsInfoPO);
    }

//    @Test
//    public void get(){
//        System.out.println(getList(0,""));
//    }


    //每页数量
    private Integer PAGESIZE=10;

    //http://localhost:8888/getGoodsList?query=商品
    //http://localhost:8888/getGoodsList?query=商品&pageNumber=1
    //根据关键字"商品"去查询列表，name或者description包含的都查询
//    public List<GoodsInfoPO> getList(Integer pageNumber, String query){
//        if(pageNumber==null) pageNumber = 0;
//        //es搜索默认第一页页码是0
//        SearchQuery searchQuery=getEntitySearchQuery(pageNumber,PAGESIZE,query);
//        Page<GoodsInfoPO> goodsPage = goodsRepository.search(searchQuery);
//        return goodsPage.getContent();
//    }


//    private SearchQuery getEntitySearchQuery(int pageNumber, int pageSize, String searchContent) {
//        FunctionScoreQueryBuilder functionScoreQueryBuilder = QueryBuilders.functionScoreQuery()
//                .add(QueryBuilders.matchPhraseQuery("name", searchContent),
//                        ScoreFunctionBuilders.weightFactorFunction(100))
//                .add(QueryBuilders.matchPhraseQuery("description", searchContent),
//                        ScoreFunctionBuilders.weightFactorFunction(100))
//                //设置权重分 求和模式
//                .scoreMode("sum")
//                //设置权重分最低分
//                .setMinScore(10);
//
//        // 设置分页
//        Pageable pageable = new PageRequest(pageNumber, pageSize);
//        return new NativeSearchQueryBuilder()
//                .withPageable(pageable)
//                .withQuery(functionScoreQueryBuilder).build();
//    }


}
