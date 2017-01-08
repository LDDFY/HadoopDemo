package com.lucene.demo02;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by LDDFY on 2017/1/8.
 */
public class IndexCreate {


    public String getFileContent(File file) {
        //获取文件内容
        StringBuffer buffer = new StringBuffer(1000);
        try {
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String tempStr = null;
            while ((tempStr = bufferedReader.readLine()) != null) {
                buffer.append(new String(tempStr.getBytes("iso-8859-1"), "GB2312"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return buffer.toString();
    }


    public void createIndex(String directoryPath, List<Map<String, String>> data) {
        System.out.println("--------------------------->开始创建索引");
        long beginTime = System.currentTimeMillis();
        //创建索引
        //创建标准分词器
        Directory directory = null;
        IndexWriter indexWriter = null;
        DirectoryReader directoryReader = null;
        try {
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            directory = FSDirectory.open(Paths.get(directoryPath));
            indexWriter = new IndexWriter(directory, config);

            for (Map<String, String> map : data) {
                Document doc = new Document();
                for (String it : map.keySet()) {
                    doc.add(new Field(it, map.get(it), TextField.TYPE_STORED));
                }
                indexWriter.addDocument(doc);
            }
            indexWriter.commit();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (indexWriter != null) {
                    indexWriter.close();
                }
                if (directory != null) {
                    directory.close();
                }
                if (directoryReader != null) {
                    directoryReader.close();
                }

                System.out.println("-------------------------->创建索引结束");
                System.out.println("时间：" + (System.currentTimeMillis() - beginTime));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 查询
     *
     * @param directoryPath 索引路径
     * @param searcherCode  查找段名称
     * @param queryStr      查找内容
     * @param limit         //前多少条内容
     */
    public void searchIndex(String directoryPath, String searcherCode, String queryStr, Integer limit) {
        System.out.println("--------------------------->开始查询");
        long beginTime = System.currentTimeMillis();
        Directory directory = null;
        DirectoryReader directoryReader = null;
        Analyzer analyzer = new StandardAnalyzer();
        try {
            directory = FSDirectory.open(Paths.get(directoryPath));
            directoryReader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(directoryReader);
            QueryParser parser = new QueryParser(searcherCode, analyzer);
            Query query = parser.parse(queryStr);
            ScoreDoc[] docs = searcher.search(query, limit).scoreDocs;

            for (int i = 0; i < docs.length; i++) {
                Document hitDoc = searcher.doc(docs[i].doc);
                System.out.println(hitDoc.get("name"));
                System.out.println(hitDoc.get("content"));
                System.out.println(hitDoc.get("url"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (directory != null) {
                    directory.close();
                }
                if (directoryReader != null) {
                    directoryReader.close();
                }
                System.out.println("--------------------------->查询结束");
                System.out.println("时间：" + (System.currentTimeMillis() - beginTime));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //打印查询内容
    public void getHitDoc(ScoreDoc[] docs,String[] printCode) {

    }

    public static void main(String[] args) {
        File file = new File("D:\\CHANGES.txt");
        IndexCreate indexCreate = new IndexCreate();
        List<Map<String, String>> datas = new ArrayList<Map<String, String>>();
        Map<String, String> data = new HashMap<String, String>();
        data.put("content", indexCreate.getFileContent(file));
        data.put("name", file.getName());
        data.put("url", file.getAbsolutePath());
        datas.add(data);
        //indexCreate.createIndex("D:\\lucene\\index\\document", datas);
        indexCreate.searchIndex("D:\\lucene\\index\\document", "content", "LUCENE", 10);
    }
}
