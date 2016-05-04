package com.mhe.novella.migration;

/**
 * @author Tarams Software Technologies
 * 
 *
 */

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.mapping.DeleteMapping;
import io.searchbox.indices.mapping.PutMapping;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;


public class UploadDocumentClient {

	public static String ELASTIC_INDEX="novella_ebook_collections_";
	public static String ELASTIC_INDEX_TYPE="novella_ebook_";
	public static Properties config=null;
	public static void main (String args[]){
		String env=System.getProperty("env");
		if(env==null){
			env= (args.length>0) ? args[0]:"local";
		}

		UploadDocumentClient jestclient= new UploadDocumentClient();
		config=jestclient.getProperties(env);
		System.out.println(config.getProperty("elasticserch_endpoint"));
		JestClient client = getElasticSearchClient();
		System.out.println("Connected to EC instance");
		String elasticSearch_type="novella_ebook_default";
		String mappingAsString="{\"properties\":{\"title\":{\"type\": \"string\"},\"data\":{\"type\": \"string\",\"analyzer\": \"novella_analyser\"},\"url\":{\"type\": \"string\"}}}";
		String settingsAsString="{\"settings\": {\"analysis\": {\"analyzer\": {\"novella_analyser\": {\"type\": \"custom\",\"tokenizer\": \"standard\",\"filter\": [\"lowercase\"]}}}}}";
		createNovellIndices(client, settingsAsString);

		StringBuilder htmlDocumentString = null;
		String[] isbns=config.getProperty("search_isbn").split(",");
		for(String isbn: isbns){
			List<String> filePath = new ArrayList<String>();
			String isbnFolderPath=config.getProperty("source_folder")+isbn;
			List<String> documentsList=new ArrayList<String>();
			if(new File(isbnFolderPath).exists()){
				elasticSearch_type = createElasticSerachMappings(client, mappingAsString, isbn);
				documentsList=getDocumentListForIsbn(new File(isbnFolderPath),filePath);
			}else{
				System.out.println("Doesnt find folder for "+isbn);
			}
			for(String documentPath: documentsList){
				System.out.println("DOCUMENT TO INDEX" + documentPath);
				htmlDocumentString = readHtmlDocument(documentPath);
				String content = getHtmlDocumentBodyContent(htmlDocumentString);
				//System.out.println(" DATA TO INDEX "+Jsoup.parse(content).body().text() );
				DocumentVO ch = createDocumentToUpload(documentPath, content, isbn);
				uploadDocumentToElasticSearch(client, elasticSearch_type,documentPath, ch);
			}
		}
		System.out.println("Completed");
	}

	private static JestClient getElasticSearchClient() {
		HttpClientConfig clientConfig = new HttpClientConfig.Builder(config.getProperty("elasticserch_endpoint")).build();
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(clientConfig);
		JestClient client = factory.getObject();
		return client;
	}

	private static String createElasticSerachMappings(JestClient client,String mappingAsString, String isbn) {
		String elasticSearch_type=ELASTIC_INDEX_TYPE+isbn;
		try {
			PutMapping putmapping = new PutMapping.Builder(ELASTIC_INDEX+config.getProperty("env"), elasticSearch_type, mappingAsString).build();
			JestResult createMappingResult = client.execute(putmapping);
			System.out.println("Created Mapping "+createMappingResult);
		} catch (Exception e2) {
			System.out.println("Exception in create mapping");
			e2.printStackTrace();
		}
		return elasticSearch_type;
	}

	private static void createNovellIndices(JestClient client,
			String settingsAsString) {
		try {
			DeleteIndex existingIndex=new DeleteIndex.Builder(ELASTIC_INDEX+config.getProperty("env")).build();
			client.execute(existingIndex);
			CreateIndex createIndex =new CreateIndex.Builder(ELASTIC_INDEX+config.getProperty("env")).settings(settingsAsString).build();
			JestResult createIndexResult=client.execute(createIndex);
			System.out.println("Created Index"+createIndexResult);
		} catch (Exception e) {
			System.out.println("Exception in createindex");
			e.printStackTrace();
		}
	}

	private static void uploadDocumentToElasticSearch(
			io.searchbox.client.JestClient client, String elasticSearch_type,
			String documentPath, DocumentVO ch) {
		Index index = new Index.Builder(ch).index(ELASTIC_INDEX+config.getProperty("env")).type(elasticSearch_type).build();
		try {
			JestResult result=client.execute(index);
			System.out.println("Add source"+result.getJsonString());
		} catch (Exception e) {
			System.out.println("Exception in create type");
			e.printStackTrace();

		}
	}

	private static DocumentVO createDocumentToUpload(String documentPath,
			String content, String isbn) {
		DocumentVO ch = new DocumentVO();

		ch.setData(content);
		ch.setTitle(documentPath.substring(documentPath.lastIndexOf(File.separator)+1,documentPath.length()).replace("_", " ").replaceAll(".htm",""));
		String documentUrl=config.getProperty("ebook_base_url")+documentPath.substring(documentPath.indexOf(isbn)).replace('\\', '/');
		ch.setUrl(documentUrl);
		return ch;
	}

	private static StringBuilder readHtmlDocument(String documentPath) {
		StringBuilder htmlDocumentString=null;
		try {
			htmlDocumentString=new StringBuilder();
			File file = new File(documentPath);
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line = reader.readLine();
			while (line != null) {
				htmlDocumentString.append(line);
				//htmlDocumentString.append(System.lineSeparator());
				line = reader.readLine();
			}
			reader.close();

		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return htmlDocumentString;
	}

	private static String getHtmlDocumentBodyContent(
			StringBuilder htmlDocumentString) {
		String bodyContent="";
		Element doc=Jsoup.parse(htmlDocumentString.toString()).body();
		if(doc.getElementById("ebBody")!=null ){
			if(doc.getElementById("ebBody").select("em")!=null){
				doc.getElementById("ebBody").select("em").remove();
			}
			String  content = doc.getElementById("ebBody").text();
			bodyContent = Jsoup.parse(content).body().text();
		}
		
		return bodyContent;
	}

	private  Properties getProperties(String env) {
		Properties prop= new Properties();
		InputStream inputStream = getClass().getClassLoader().getResourceAsStream(env+".properties");
		try {
			prop.load(inputStream);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return prop;
	}

	public static List<String> getDocumentListForIsbn(final File folder,List<String> filePath ) {
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				getDocumentListForIsbn(fileEntry,filePath);
			} else {
				if(fileEntry.getName().endsWith(".htm")|| fileEntry.getName().endsWith(".html")){
					filePath.add(fileEntry.getAbsolutePath());
				}
				System.out.println(fileEntry.getAbsolutePath());
			}
		}
		return filePath;
	}

}
