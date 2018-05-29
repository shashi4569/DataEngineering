
import sys
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext 


if __name__ == "__main__":
    sc = SparkContext(appName="sparkLogStreamingConsumer")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 10) # 2 second window
    #ssc.checkpoint('/output/checkpoint')
    kvs = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'test1':1}) 
    #kvs = KafkaUtils.createDirectStream(ssc, 'test1', {"metadata.broker.list": 'quickstart.cloudera:9092'})
    lines = kvs.map(lambda x: x[1])
    counts = lines.map(lambda line: line.split("- -"))
    counts2 = counts.map(lambda x: (x[0],1)).reduceByKey(lambda a, b: a + b).filter(lambda (k,v): v >= 10)
    counts2.pprint()
    counts2.saveAsTextFiles('/output/ddos_attacks/DDOS_attack_ipaddress')
    ssc.start()
    ssc.awaitTermination()

