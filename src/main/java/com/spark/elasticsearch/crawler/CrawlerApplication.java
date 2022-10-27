package com.spark.elasticsearch.crawler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CrawlerApplication {

	public static void main(String[] args) {
		SpringApplication.run(CrawlerApplication.class, args);

//		System.setProperty("HADOOP_HOME", "C:\\niagara\\elk\\hadoop");
//		System.setProperty("hadoop.home.dir", "C:\\niagara\\elk\\hadoop");
	}

}
