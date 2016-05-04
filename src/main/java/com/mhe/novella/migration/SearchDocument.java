package com.mhe.novella.migration;

import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class SearchDocument {
	public static String ELASTIC_INDEX="novella_ebook_collections";
	public static String ELASTIC_INDEX_TYPE="novella_ebook_0077268830";

	///home/tarams/migration/novella-es-migration-0.0.1-jar-with-dependencies.jar

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SearchDocument updateDoc= new SearchDocument();
		//Properties config=updateDoc.getProperties();
		//System.out.println(config.getProperty("elasticserch_endpoint"));
		//HttpClientConfig clientConfig = new HttpClientConfig.Builder("http://search-taramsdomain-ias6i6kdc3lrlsizumupdop33i.us-east-1.es.amazonaws.com").build();
		HttpClientConfig clientConfig = new HttpClientConfig.Builder("http://localhost:9200").build();
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(clientConfig);
		io.searchbox.client.JestClient client = factory.getObject();
		//String elasticSearch_type=ELASTIC_INDEX_TYPE+config.getProperty("search_isbn");
		
		String query_string_data="{\"fields\": [\"title\",\"data\"],\"query\":{\"match\":{\"data\":\"chapter\"}},\"highlight\":{\"fields\": {\"data\": {\"fragment_size\":150,\"number_of_fragments\":5}}}}}";
		Search search=new Search.Builder(query_string_data).addIndex(ELASTIC_INDEX).addType(ELASTIC_INDEX_TYPE).build();
		try {
			JestResult result=client.execute(search);
			JsonObject object= (JsonObject)result.getJsonObject();
			JsonArray array=object.getAsJsonObject("hits").getAsJsonArray("hits");
				for (JsonElement doc : array){
					JsonArray dataFragment = doc.getAsJsonObject().get("highlight").getAsJsonObject().get("data").getAsJsonArray();
					for (JsonElement jsonElement : dataFragment) {
						System.out.println(jsonElement.getAsString());
					}
					System.out.println("=====================================================");
				}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Exception in searching");
			e.printStackTrace();

		}
	}
	private  Properties getProperties() {
		Properties prop= new Properties();
		InputStream inputStream = getClass().getClassLoader().getResourceAsStream("config.properties");
		try {
			prop.load(inputStream);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return prop;
	}

}
