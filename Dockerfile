FROM openjdk:8-jre-alpine
ADD /target/sparktest-jar-with-dependencies.jar test.jar
ENTRYPOINT ["java","-jar","test.jar"]