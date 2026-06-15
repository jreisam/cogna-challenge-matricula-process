# Read Me First
The following was discovered as part of building this project:

* The original package name 'com.cogna.matricula-process' is invalid and this project uses 'com.cogna.matricula_process' instead.

# Getting Started

### Reference Documentation
For further reference, please consider the following sections:

* [Official Gradle documentation](https://docs.gradle.org)
* [Spring Boot Gradle Plugin Reference Guide](https://docs.spring.io/spring-boot/3.5.15/gradle-plugin)
* [Create an OCI image](https://docs.spring.io/spring-boot/3.5.15/gradle-plugin/packaging-oci-image.html)
* [Spring Boot Testcontainers support](https://docs.spring.io/spring-boot/3.5.15/reference/testing/testcontainers.html#testing.testcontainers)
* [Testcontainers MongoDB Module Reference Guide](https://java.testcontainers.org/modules/databases/mongodb/)
* [Testcontainers Kafka Modules Reference Guide](https://java.testcontainers.org/modules/kafka/)
* [Spring for Apache Kafka](https://docs.spring.io/spring-boot/3.5.15/reference/messaging/kafka.html)
* [Spring Web](https://docs.spring.io/spring-boot/3.5.15/reference/web/servlet.html)
* [Spring Data MongoDB](https://docs.spring.io/spring-boot/3.5.15/reference/data/nosql.html#data.nosql.mongodb)
* [Spring Boot Actuator](https://docs.spring.io/spring-boot/3.5.15/reference/actuator/index.html)
* [Validation](https://docs.spring.io/spring-boot/3.5.15/reference/io/validation.html)
* [Spring Boot DevTools](https://docs.spring.io/spring-boot/3.5.15/reference/using/devtools.html)
* [Testcontainers](https://java.testcontainers.org/)

### Guides
The following guides illustrate how to use some features concretely:

* [Building a RESTful Web Service](https://spring.io/guides/gs/rest-service/)
* [Serving Web Content with Spring MVC](https://spring.io/guides/gs/serving-web-content/)
* [Building REST services with Spring](https://spring.io/guides/tutorials/rest/)
* [Accessing Data with MongoDB](https://spring.io/guides/gs/accessing-data-mongodb/)
* [Building a RESTful Web Service with Spring Boot Actuator](https://spring.io/guides/gs/actuator-service/)
* [Validation](https://spring.io/guides/gs/validating-form-input/)

### Additional Links
These additional references should also help you:

* [Gradle Build Scans – insights for your project's build](https://scans.gradle.com#gradle)

### Testcontainers support

This project uses [Testcontainers at development time](https://docs.spring.io/spring-boot/3.5.15/reference/features/dev-services.html#features.dev-services.testcontainers).

Testcontainers has been configured to use the following Docker images:

* [`apache/kafka-native:latest`](https://hub.docker.com/r/apache/kafka-native)
* [`mongo:latest`](https://hub.docker.com/_/mongo)

Please review the tags of the used images and set them to the same as you're running in production.

