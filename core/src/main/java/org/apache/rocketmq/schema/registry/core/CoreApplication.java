package org.apache.rocketmq.schema.registry.core;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.FilterType;
import org.springframework.scheduling.annotation.EnableScheduling;

import springfox.documentation.oas.annotations.EnableOpenApi;

@SpringBootApplication
@EnableScheduling
@EnableOpenApi
@ComponentScan(excludeFilters = @Filter(type = FilterType.ASPECTJ, pattern = "org.apache.rocketmq.schema.registry.storage..*"))
public class CoreApplication {

  public CoreApplication() {
  }

  public static void main(String[] args) {
    SpringApplication.run(CoreApplication.class, args);
  }

}
