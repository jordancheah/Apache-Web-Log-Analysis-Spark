# Web server log analysis with Apache Spark

This program illustrates how to use Apache Spark on real-world text-based production logs.

Server log analysis is an ideal use case for Spark.  
It's a very large, common data source and contains a rich set of information.  
Spark allows you to store your logs in files on disk cheaply, while still providing a quick and simple way 
to perform data analysis on them.  


Log data comes from many sources, such as web, file, and compute servers, application logs, 
user-generated content,  and can be used for monitoring servers, improving business and customer
intelligence, building recommendation systems, fraud detection, and much more.


### Data File: Apache Common Log Format (CLF)
http://httpd.apache.org/docs/1.3/logs.html#common
 
The log file entries produced in CLF will look something like this:
```
127.0.0.1 - - [01/Aug/1995:00:00:01 -0400] "GET /images/launch-logo.gif HTTP/1.0" 200 1839
```