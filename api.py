
from kafka import KafkaProducer
import pandas as pd
import json
import time

# Kafka Producer ayarları
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Kafka sunucusunun adresi
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Veri setini yükle
salary_data = pd.read_csv('test.csv')

# Veri setindeki her satır için bir Kafka mesajı gönder
for index, row in salary_data.iterrows():
    message = {
        'Age': row['Age'],
        'Gender': row['Gender'],
        'Education Level': row['Education Level'],
        'Job Title': row['Job Title'],
        'Years of Experience': row['Years of Experience'],
        'Salary': row['Salary']
    }
    producer.send('salary_topic', value=message)
    time.sleep(5)  # Mesajlar arasında kısa bir bekleme süresi

# Kafka Producer'ı kapat
producer.close()
