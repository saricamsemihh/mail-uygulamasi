import pika
import json
import os

# --- Bağlantı Ayarları ---
# Bu bölüm, kodun hem lokalde hem de canlı sunucuda (production)
# değişiklik yapmadan çalışmasını sağlar.

# 1. Lokal (Geliştirme) için varsayılan RabbitMQ adresi.
LOCAL_RABBITMQ_URL = 'amqps://ivrivlyb:QhfouMJbiMktA6-Djs1FNWdwRIiZFma4@cow.rmq2.cloudamqp.com/ivrivlyb'

# 2. Canlı ortam için ortam değişkenlerinden (environment variable) URL'yi okumaya çalışır.
#    Eğer 'CLOUDAMQP_URL' isminde bir değişken bulamazsa, lokal adresi kullanır.
#    Bu, Render gibi hosting servisleri için kritiktir.
RABBITMQ_URL = os.environ.get('CLOUDAMQP_URL', LOCAL_RABBITMQ_URL)

# 3. Tüm fonksiyonlarda kullanılacak ortak kuyruk adı.
QUEUE_NAME = 'mail_queue'


def send_message_to_queue(message: dict):
    """
    Verilen mesajı (dictionary) alıp RabbitMQ kuyruğuna gönderir.
    """
    try:
        # Bağlantıyı URL üzerinden kurar. Bu, hem lokal (amqp://) hem de
        # bulut (amqps://) bağlantılarını destekler.
        params = pika.URLParameters(RABBITMQ_URL)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        # Kuyruğun var olduğundan ve kalıcı (durable) olduğundan emin olur.
        channel.queue_declare(queue=QUEUE_NAME, durable=True)

        # Python dictionary'sini JSON formatında bir metne çevirir.
        message_body = json.dumps(message)

        # Mesajı, standartlara uygun özelliklerle kuyruğa yayınlar.
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
            body=message_body,
            properties=pika.BasicProperties(
                delivery_mode=2,  # Mesajın kalıcı olmasını sağlar.
                content_type='application/json'  # Mesaj içeriğinin tipini belirtir.
            )
        )
        print(f"Mesaj '{QUEUE_NAME}' kuyruğuna başarıyla gönderildi.")

    except pika.exceptions.AMQPConnectionError as e:
        print(f"HATA: RabbitMQ sunucusuna bağlanılamadı. URL: {RABBITMQ_URL} - Hata: {e}")
    finally:
        # Bağlantının açık olup olmadığını kontrol edip kapatır.
        if 'connection' in locals() and connection.is_open:
            connection.close()


def receive_messages_from_queue(receiver_id: int):
    """
    Belirli bir alıcıya ait mesajları kuyruktan okur.
    """
    messages = []
    try:
        # Bağlantıyı 'send_message_to_queue' ile aynı esnek yöntemle kurar.
        params = pika.URLParameters(RABBITMQ_URL)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        channel.queue_declare(queue=QUEUE_NAME, durable=True)

        # Bu fonksiyon, kuyruktaki tüm mesajları tek tek kontrol eder.
        # Bu yöntem küçük uygulamalar için uygundur.
        while True:
            # Kuyruktan bir mesaj çekmeye çalışır (beklemeden).
            method_frame, properties, body = channel.basic_get(queue=QUEUE_NAME)

            # Eğer kuyrukta mesaj yoksa, döngüden çıkar.
            if method_frame is None:
                break

            message = json.loads(body)

            # Mesaj doğru alıcıya aitse...
            if message.get('receiver_id') == receiver_id:
                messages.append(message)
                # Mesajı işlediğimizi ve kuyruktan silebileceğini bildiririz.
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            # Mesaj başkasına aitse...
            else:
                # Mesajı tekrar kuyruğa koyması için (işlemedik olarak) bildiririz.
                channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)

    except pika.exceptions.AMQPConnectionError as e:
        print(f"HATA: RabbitMQ sunucusuna bağlanılamadı. URL: {RABBITMQ_URL} - Hata: {e}")
        # Hata durumunda boş bir liste döndürür.
        return []
    finally:
        # Bağlantının açık olup olmadığını kontrol edip kapatır.
        if 'connection' in locals() and connection.is_open:
            connection.close()

    return messages
