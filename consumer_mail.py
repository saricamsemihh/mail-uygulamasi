import pika
import json
import os
import time

print("Consumer başlatılıyor, mesajlar bekleniyor...")


def process_message(ch, method, properties, body):
    """Bir mesaj alındığında bu fonksiyon çalışır."""
    try:
        # Mesajın gövdesini (body) JSON formatından Python dictionary'e çevir
        message_data = json.loads(body)
        print(f" [✔] Mesaj alındı:")
        print(f"     Gönderen:    {message_data.get('sender_email')}")
        print(f"     Alıcı:       {message_data.get('receiver_email')}")
        print(f"     Konu:        {message_data.get('subject')}")
        print(f"     İçerik:      {message_data.get('content')}")
        print("-------------------------------------------------")

        # Mesajın başarıyla işlendiğini RabbitMQ'ya bildiriyoruz.
        # Bu, mesajın kuyruktan silinmesini sağlar.
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError:
        print(" [X] Hatalı formatta mesaj alındı, mesaj işlenemedi.")
        # Hatalı mesajı reddet ve tekrar kuyruğa aldırma (requeue=False)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        print(f" [X] Mesaj işlenirken bir hata oluştu: {e}")
        # Hata durumunda mesajı reddet
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def start_consuming():
    """RabbitMQ'ya bağlanır ve mesajları dinlemeye başlar."""
    rabbitmq_url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost:5672')
    params = pika.URLParameters(rabbitmq_url)

    while True:
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue='mail_queue', durable=True)

            # Sadece birer birer mesaj al (prefetch_count=1)
            channel.basic_qos(prefetch_count=1)

            # 'mail_queue' kuyruğunu dinle ve mesaj gelince 'process_message' fonksiyonunu çalıştır
            channel.basic_consume(queue='mail_queue', on_message_callback=process_message)

            print("Kuyruk dinleniyor. Çıkmak için CTRL+C'ye basın.")
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            print(f"Bağlantı hatası: {e}. 5 saniye içinde tekrar denenecek...")
            time.sleep(5)
        except KeyboardInterrupt:
            print("Consumer durduruldu.")
            break


if __name__ == '__main__':
    start_consuming()


#Bir terminalde FastAPI sunucunu çalıştır: 'uvicorn main:app --reload'
#Yeni bir terminal aç ve aynı proje klasöründeyken consumer'ı çalıştır: python consumer.py
#Artık API üzerinden bir mesaj gönderdiğinde, consumer'ın çalıştığı terminal ekranına mesajın detaylarının basıldığını göreceksin.