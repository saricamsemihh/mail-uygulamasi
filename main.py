# Gerekli kütüphaneleri ve modülleri içeri aktarıyoruz.
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr  # Pydantic'ten e-posta formatını doğrulayan EmailStr'i ekliyoruz.
from typing import List
from datetime import datetime
import json
from fastapi.middleware.cors import CORSMiddleware
import os
import mail_database
from dotenv import load_dotenv
load_dotenv()

# Kendi yazdığımız RabbitMQ yardımcı fonksiyonlarını içeri aktarıyoruz.
from RabbitMQ_yardimci import send_message_to_queue, receive_messages_from_queue

# FastAPI uygulamasının ana nesnesini oluşturuyoruz.
app = FastAPI()

@app.on_event("startup")
def on_startup():
    """Uygulama ilk çalıştığında bu fonksiyon tetiklenir."""
    mail_database.create_database_and_tables()

# --- CORS Ayarları ---
# Frontend'den (Necati'nin arayüzünden) gelecek isteklere izin vermek için gereklidir.
origins = [
    "http://localhost:5500",
    "http://127.0.0.1:5500",
    # Canlıya çıkıldığında frontend'in adresi de buraya eklenecek.
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ------------------------------------------------------------------------------------
# Veri Modelleri (Pydantic ile)
# API'ye gelen verilerin yapısını ve doğruluğunu kontrol ederler.
# ------------------------------------------------------------------------------------

class User(BaseModel):
    id: int
    name: str
    email: EmailStr  # Gelen e-postanın geçerli bir formatta olmasını zorunlu kılar.


class NewUser(BaseModel):
    name: str
    email: EmailStr


# Mesaj modeli artık ID yerine e-posta adresleri alacak şekilde güncellendi.
class Message(BaseModel):
    sender_email: EmailStr
    receiver_email: EmailStr
    subject: str
    content: str
    timestamp: str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# ------------------------------------------------------------------------------------
# Başlangıç Verisi ve Yardımcı Yapılar
# ------------------------------------------------------------------------------------

# Uygulama başladığında, kullanıcı bilgilerini "kullanicilar.json" dosyasından okuyoruz.
with open("kullanicilar.json", "r", encoding="utf-8") as f:
    users_data = json.load(f)

# E-posta ve ID'lerden kullanıcı bilgilerine hızlıca ulaşmak için sözlük (map) yapıları oluşturuyoruz.
# Bu, her istekte tüm listeyi gezmekten çok daha verimlidir.
email_to_user_map = {user["email"]: user for user in users_data}
id_to_user_map = {user["id"]: user for user in users_data}


# ------------------------------------------------------------------------------------
# API Endpoint'leri
# ------------------------------------------------------------------------------------

@app.get("/")
def read_root():
    return {"mesaj": "Mail Uygulaması API'sine Hoş Geldiniz!"}


@app.get("/users", response_model=List[User])
def get_users():
    return users_data


@app.post("/users")
def register_user(new_user: NewUser):
    """Yeni bir kullanıcıyı sisteme kaydeder ve kullanicilar.json dosyasını günceller."""

    # Not: Gerçek bir uygulamada, bu global değişkenlerin de güncellenmesi gerekir.
    # Bu basit örnekte, dosya her seferinde yeniden okunur.

    # E-postanın zaten kayıtlı olup olmadığını kontrol et
    if new_user.email in email_to_user_map:
        raise HTTPException(status_code=400, detail="Bu e-posta adresi zaten kayıtlı.")

    with open("kullanicilar.json", "r+", encoding="utf-8") as f:
        try:
            current_users = json.load(f)
        except json.JSONDecodeError:
            current_users = []

        # Yeni kullanıcı için bir ID oluştur (mevcut en yüksek ID'nin bir fazlası)
        new_id = max([user['id'] for user in current_users]) + 1 if current_users else 1

        # Yeni kullanıcıyı dictionary formatına çevir
        user_dict = {
            "id": new_id,
            "name": new_user.name,
            "email": new_user.email
        }

        # Listeye yeni kullanıcıyı ekle
        current_users.append(user_dict)

        # Dosyanın imlecini en başa al ve güncel listeyi dosyaya yaz
        f.seek(0)
        json.dump(current_users, f, indent=2, ensure_ascii=False)
        f.truncate()

    # Başarılı yanıtı ve yeni kullanıcıyı geri dön
    return {"status": "Kullanıcı başarıyla oluşturuldu.", "user": user_dict}


@app.post("/messages")
def send_message(msg: Message):
    sender = email_to_user_map.get(msg.sender_email)
    receiver = email_to_user_map.get(msg.receiver_email)

    if not sender or not receiver:
        raise HTTPException(status_code=404, detail="Gönderici veya alıcı e-postası bulunamadı.")

    # Mesajı hem veritabanına hem de RabbitMQ'ya göndereceğimiz formatı hazırlıyoruz.
    message_data = {
        "sender_id": sender["id"],
        "receiver_id": receiver["id"],
        "sender_email": msg.sender_email,
        "receiver_email": msg.receiver_email,
        "subject": msg.subject,
        "content": msg.content,
        "timestamp": msg.timestamp
    }

    # 1. ADIM: Mesajı kalıcı olarak veritabanına kaydet.
    mail_database.save_message_to_db(message_data)

    # 2. ADIM (Opsiyonel ama iyi pratik): Anlık bildirim gibi işler için
    #    mesajı RabbitMQ'ya da göndererek consumer'ları tetikle.
    send_message_to_queue(message_data)

    return {"status": "Mesaj başarıyla gönderildi ve kaydedildi."}


@app.get("/messages/{user_email}")
def get_messages(user_email: EmailStr):
    """Belirli bir kullanıcının mesajlarını artık RabbitMQ yerine VERİTABANINDAN çeker."""

    target_user = email_to_user_map.get(user_email)
    if not target_user:
        raise HTTPException(status_code=404, detail=f"{user_email} adresiyle kayıtlı kullanıcı bulunamadı.")

    receiver_id = target_user["id"]

    # Artık RabbitMQ'dan değil, doğrudan veritabanından mesaj geçmişini okuyoruz.
    db_messages = mail_database.get_messages_from_db(receiver_id)

    # Frontend'e göndermek için veriyi zenginleştiriyoruz.
    enriched_messages = []
    for msg in db_messages:
        sender_info = id_to_user_map.get(msg["sender_id"])
        if sender_info:
            enriched_msg = {
                "sender_name": sender_info.get("name"),
                "subject": msg["subject"],
                "content": msg["content"],
                "timestamp": msg["timestamp"]
            }
            enriched_messages.append(enriched_msg)

    return {"messages": enriched_messages}
