package output.writers;


import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ElasticSearchOutputWriter<T extends SerializableData> extends AbstractOutputChannel<T>{

    private final String objectType;
    private final boolean reIndex;
    JestClientFactory factory = new JestClientFactory();

    JestClient client;

    List<Index> buffer =new ArrayList<>(5000);

    String indexName;




    public ElasticSearchOutputWriter(String urlWithPort, String indexName, boolean reIndex, String objectType){
//        this.mode=OutputFormat.JSON;
        this.reIndex=reIndex;
        this.objectType=objectType;
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(urlWithPort)
                .multiThreaded(true)
                .connTimeout(3000000)
                .readTimeout(300000)
                .build());
        client = factory.getObject();


        this.indexName=indexName;
        boolean indexExists = false;
        try {
            indexExists = client.execute(new IndicesExists.Builder(indexName).build()).isSucceeded();
            if (reIndex && indexExists) {
                client.execute(new DeleteIndex.Builder(indexName).build());
                client.execute(new CreateIndex.Builder(indexName).build());
            }
////            else{
//
//            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Collection<T> records) {
        try {
            Bulk.Builder bulkIndexBuilder = new Bulk.Builder();
            for (T record : records) {

                bulkIndexBuilder.addAction(new Index.Builder(record).index(indexName).type(objectType).build());
            }
            BulkResult res = client.execute(bulkIndexBuilder.build());

//            System.out.println(res.getResponseCode()+" "+res.getErrorMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void write(T record) {
//        try {
            Index index = new Index.Builder(record).index(indexName).type(objectType).build();
//            client.execute(index);
            addToBuffer(index);

//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    private synchronized void addToBuffer(Index index) {
        buffer.add(index);
        if (buffer.size()>1000){
            flushBuffer();
        }
    }

    private synchronized void flushBuffer() {
        try {
            Bulk.Builder bulkIndexBuilder = new Bulk.Builder();
            buffer.forEach(i-> bulkIndexBuilder.addAction(i));
            BulkResult res = client.execute(bulkIndexBuilder.build());
            buffer=new ArrayList<>(5000);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public boolean close() {
            flushBuffer();
        return true;
    }

    @Override
    public String getName() {
        return "elastic-"+indexName+"-"+objectType;
    }

}
