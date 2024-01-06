Ubuntu için anlatımdır --
Çalıştırılması :
Zookeeper başlatınız. // Kafka klasörünün ana dizininde : zookeeper-server-start.sh config/zookeeper.properties
Broker başlatınız. // Kafka klasörünün ana dizininde : kafka-server-start.sh config/server.properties
salary_topic adında bir topic oluşturunuz. // broker başlatıldıktan sonra kafka klasörünün ana dizininde : kafka-topics.sh --create --topic salary_topic --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
Dosya dizininde : python api.py kodu ile kafkadan streaming başlatınız.
Dosya dizininde : spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:[3.5.0] sparkSes.py
Veri kontrolü localhost:4040 adresinden takip edilebilir. Ayrıca işlenen veri terminal ekranından izlenebilir.