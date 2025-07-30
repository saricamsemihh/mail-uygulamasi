import sqlite3

DATABASE_NAME = "mail_database.db"


def get_db_connection():
    """Veritabanına bir bağlantı oluşturur ve geri döndürür."""
    conn = sqlite3.connect(DATABASE_NAME)
    # Sonuçları sözlük (dictionary) formatında almak için row_factory ayarı
    conn.row_factory = sqlite3.Row
    return conn


def create_database_and_tables():
    """
    Veritabanı dosyasını ve içinde mesajları saklayacağımız 'messages' tablosunu oluşturur.
    Eğer tablo zaten varsa, hiçbir şey yapmaz.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Mesajları saklamak için bir tablo oluşturuyoruz.
    # ID: Her mesaj için otomatik artan benzersiz bir numara.
    # sender_id, receiver_id: Mesajı gönderen ve alan kullanıcıların ID'leri.
    # subject, content, timestamp: Mesajın kendisi.
    cursor.execute("\n"
                   "                   CREATE TABLE IF NOT EXISTS messages\n"
                   "                   (\n"
                   "                       id\n"
                   "                       INTEGER\n"
                   "                       PRIMARY\n"
                   "                       KEY\n"
                   "                       AUTOINCREMENT,\n"
                   "                       sender_id\n"
                   "                       INTEGER\n"
                   "                       NOT\n"
                   "                       NULL,\n"
                   "                       receiver_id\n"
                   "                       INTEGER\n"
                   "                       NOT\n"
                   "                       NULL,\n"
                   "                       subject\n"
                   "                       TEXT\n"
                   "                       NOT\n"
                   "                       NULL,\n"
                   "                       content\n"
                   "                       TEXT\n"
                   "                       NOT\n"
                   "                       NULL,\n"
                   "                       timestamp\n"
                   "                       TEXT\n"
                   "                       NOT\n"
                   "                       NULL\n"
                   "                   )\n"
                   "                   ")

    conn.commit()
    conn.close()
    print("Veritabanı ve tablolar başarıyla oluşturuldu veya zaten mevcut.")


def save_message_to_db(message_data: dict):
    """
    Yeni bir mesajı veritabanındaki 'messages' tablosuna kaydeder.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
                   INSERT INTO messages (sender_id, receiver_id, subject, content, timestamp)
                   VALUES (?, ?, ?, ?, ?)
                   """, (
                       message_data['sender_id'],
                       message_data['receiver_id'],
                       message_data['subject'],
                       message_data['content'],
                       message_data['timestamp']
                   ))

    conn.commit()
    conn.close()
    print(f"Mesaj, veritabanına başarıyla kaydedildi.")


def get_messages_from_db(receiver_id: int):
    """
    Belirli bir alıcıya ait tüm mesajları veritabanından çeker.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Alıcı ID'sine göre tüm mesajları, en yeniden en eskiye doğru sıralayarak seçer.
    cursor.execute("""
                   SELECT *
                   FROM messages
                   WHERE receiver_id = ?
                   ORDER BY timestamp DESC
                   """, (receiver_id,))

    # Gelen tüm satırları bir liste olarak alır.
    messages = cursor.fetchall()

    conn.close()

    # sqlite3.Row nesnelerini normal dictionary'lere çevirerek döndürür.
    return [dict(row) for row in messages]