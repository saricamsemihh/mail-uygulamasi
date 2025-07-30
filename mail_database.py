import os
import psycopg2
from psycopg2.extras import RealDictCursor

# --- Bağlantı Ayarları ---
# Render'da ayarladığımız DATABASE_URL ortam değişkenini okur.
# Bu, veritabanı şifresi gibi hassas bilgileri kodun dışında tutmamızı sağlar.
DATABASE_URL = os.environ.get('DATABASE_URL')

def get_db_connection():
    """
    Supabase (PostgreSQL) veritabanına bir bağlantı oluşturur ve geri döndürür.
    """
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except psycopg2.OperationalError as e:
        print(f"VERİTABANI BAĞLANTI HATASI: {e}")
        # Bağlantı başarısız olursa None döndürür.
        return None

def create_database_and_tables():
    """
    Veritabanında 'messages' tablosunu oluşturur.
    Eğer tablo zaten varsa, hiçbir şey yapmaz.
    """
    conn = get_db_connection()
    if conn is None:
        return # Bağlantı kurulamadıysa fonksiyondan çık.

    # 'with' bloğu, işlem sonunda bağlantının ve cursor'ın otomatik olarak
    # kapatılmasını sağlar.
    with conn.cursor() as cursor:
        # PostgreSQL'de otomatik artan ID için 'SERIAL PRIMARY KEY' kullanılır.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id SERIAL PRIMARY KEY,
                sender_id INTEGER NOT NULL,
                receiver_id INTEGER NOT NULL,
                subject TEXT NOT NULL,
                content TEXT NOT NULL,
                timestamp TEXT NOT NULL
            )
        """)
        conn.commit()
    conn.close()
    print("Veritabanı ve tablolar başarıyla oluşturuldu veya zaten mevcut.")

def save_message_to_db(message_data: dict):
    """
    Yeni bir mesajı veritabanındaki 'messages' tablosuna kaydeder.
    """
    conn = get_db_connection()
    if conn is None:
        return

    with conn.cursor() as cursor:
        # PostgreSQL'de parametreler için '?' yerine '%s' kullanılır.
        cursor.execute("""
            INSERT INTO messages (sender_id, receiver_id, subject, content, timestamp)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            message_data['sender_id'],
            message_data['receiver_id'],
            message_data['subject'],
            message_data['content'],
            message_data['timestamp']
        ))
        conn.commit()
    conn.close()
    print(f"Mesaj, PostgreSQL veritabanına başarıyla kaydedildi.")

def get_messages_from_db(receiver_id: int):
    """
    Belirli bir alıcıya ait tüm mesajları veritabanından çeker.
    """
    conn = get_db_connection()
    if conn is None:
        return [] # Hata durumunda boş liste döndür.

    # RealDictCursor, sonuçları sözlük (dictionary) formatında almamızı sağlar.
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""
            SELECT * FROM messages 
            WHERE receiver_id = %s 
            ORDER BY timestamp DESC
        """, (receiver_id,))

        # Gelen tüm satırları bir liste olarak alır.
        messages = cursor.fetchall()

    conn.close()
    return messages