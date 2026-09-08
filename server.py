import os
import uuid
import json
import hashlib
import shutil
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, UploadFile, File, Form, Query, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field, validator, ConfigDict
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, ForeignKey, Text, Index, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from sqlalchemy.sql import func
from passlib.context import CryptContext
import jwt
from datetime import timezone
import bcrypt
import aiofiles
import aiofiles.os as aio_os
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import pytz

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60
REFRESH_TOKEN_EXPIRE_DAYS = 7
UPLOAD_TEMP_DIR = "uploads/temp"
UPLOAD_AVATARS_DIR = "uploads/avatars"
MAX_FILE_SIZE = 500 * 1024  # 500 KB
ALLOWED_FILE_TYPES = ['image/jpeg', 'image/png', 'image/gif', 'application/pdf', 'text/plain']

# Создание директорий
os.makedirs(UPLOAD_TEMP_DIR, exist_ok=True)
os.makedirs(UPLOAD_AVATARS_DIR, exist_ok=True)

# База данных
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./artmessage.db")
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Безопасность
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer()

# Модели Pydantic для валидации
class UserCreate(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    first_name: str = Field(..., min_length=1, max_length=100)
    last_name: Optional[str] = Field(None, max_length=100)
    bio: Optional[str] = Field(None, max_length=500)
    password: str = Field(..., min_length=6)
    public_key: str = Field(..., min_length=10)
    hardware_id: str = Field(..., min_length=10)
    avatar: Optional[UploadFile] = None

class UserLogin(BaseModel):
    username: str
    password: str
    hardware_id: str

class UserUpdate(BaseModel):
    username: Optional[str] = Field(None, min_length=3, max_length=50)
    first_name: Optional[str] = Field(None, min_length=1, max_length=100)
    last_name: Optional[str] = Field(None, max_length=100)
    bio: Optional[str] = Field(None, max_length=500)

class PasswordChange(BaseModel):
    old_password: str
    new_password: str = Field(..., min_length=6)

class ChatCreate(BaseModel):
    type: str = Field(..., regex="^(private|group|channel)$")
    name: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = Field(None, max_length=500)
    user_ids: Optional[List[int]] = []

class MessageCreate(BaseModel):
    content: str = Field(..., min_length=1)
    message_id: str = Field(..., min_length=10)  # UUID от клиента
    send_id: str = Field(..., min_length=10)     # UUID от клиента
    file_url: Optional[str] = None

class MessageRead(BaseModel):
    message_id: str
    send_id: str

class FolderUpdate(BaseModel):
    folder: Optional[str] = Field(None, max_length=50)

class MemberAdd(BaseModel):
    user_id: int

class MemberRole(BaseModel):
    role: str = Field(..., regex="^(admin|member|creator)$")

class Token(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"

class TokenRefresh(BaseModel):
    refresh_token: str

# SQLAlchemy модели
class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        Index('ix_users_username', 'username'),
        Index('ix_users_hardware_id', 'hardware_id'),
    )

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=True)
    bio = Column(Text, nullable=True)
    password_hash = Column(String(255), nullable=False)
    avatar_url = Column(String(255), nullable=True)
    public_key = Column(Text, nullable=False)
    hardware_id = Column(String(255), unique=True, nullable=False, index=True)
    is_online = Column(Boolean, default=False)
    last_seen = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Отношения
    sent_messages = relationship("Message", foreign_keys="Message.sender_id", back_populates="sender")
    chat_memberships = relationship("ChatMember", back_populates="user")
    blocked_users = relationship("UserBlock", foreign_keys="UserBlock.blocker_id", back_populates="blocker")
    blocked_by = relationship("UserBlock", foreign_keys="UserBlock.blocked_id", back_populates="blocked")
    created_chats = relationship("Chat", back_populates="creator")

class Chat(Base):
    __tablename__ = "chats"

    id = Column(Integer, primary_key=True, index=True)
    type = Column(String(20), nullable=False)  # private, group, channel
    name = Column(String(100), nullable=True)
    description = Column(Text, nullable=True)
    avatar_url = Column(String(255), nullable=True)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Отношения
    creator = relationship("User", foreign_keys=[created_by], back_populates="created_chats")
    members = relationship("ChatMember", back_populates="chat")
    messages = relationship("Message", back_populates="chat")

class ChatMember(Base):
    __tablename__ = "chat_members"
    __table_args__ = (
        UniqueConstraint('chat_id', 'user_id', name='uq_chat_member'),
    )

    id = Column(Integer, primary_key=True, index=True)
    chat_id = Column(Integer, ForeignKey("chats.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    role = Column(String(20), default="member")  # admin, member, creator
    is_pinned = Column(Boolean, default=False)
    folder = Column(String(50), nullable=True)
    joined_at = Column(DateTime, default=datetime.utcnow)

    # Отношения
    chat = relationship("Chat", back_populates="members")
    user = relationship("User", back_populates="chat_memberships")

class Message(Base):
    __tablename__ = "messages"
    __table_args__ = (
        Index('ix_messages_chat_id', 'chat_id'),
        Index('ix_messages_sender_id', 'sender_id'),
        Index('ix_messages_created_at', 'created_at'),
        Index('ix_messages_send_id', 'send_id'),
    )

    id = Column(Integer, primary_key=True, index=True)
    message_id = Column(String(36), unique=True, nullable=False, index=True)  # UUID клиента
    send_id = Column(String(36), nullable=False, index=True)  # UUID для пары отправитель-получатель
    chat_id = Column(Integer, ForeignKey("chats.id"), nullable=False)
    sender_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    content = Column(Text, nullable=False)  # Зашифрованный текст
    file_url = Column(String(255), nullable=True)
    file_type = Column(String(50), nullable=True)  # image, document
    file_name = Column(String(255), nullable=True)
    is_read = Column(Boolean, default=False)
    read_at = Column(DateTime, nullable=True)
    delivered_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Отношения
    chat = relationship("Chat", back_populates="messages")
    sender = relationship("User", foreign_keys=[sender_id], back_populates="sent_messages")

class UserBlock(Base):
    __tablename__ = "user_blocks"
    __table_args__ = (
        UniqueConstraint('blocker_id', 'blocked_id', name='uq_user_block'),
    )

    id = Column(Integer, primary_key=True, index=True)
    blocker_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    blocked_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Отношения
    blocker = relationship("User", foreign_keys=[blocker_id], back_populates="blocked_users")
    blocked = relationship("User", foreign_keys=[blocked_id], back_populates="blocked_by")

# Создание таблиц
Base.metadata.create_all(bind=engine)

# Вспомогательные функции
def get_db():
    """Получение сессии базы данных"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Проверка пароля"""
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    """Хеширование пароля"""
    return pwd_context.hash(password)

def create_access_token(data: dict, expires_delta: timedelta = None):
    """Создание access токена"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire, "type": "access"})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def create_refresh_token(data: dict):
    """Создание refresh токена"""
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire, "type": "refresh"})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def verify_token(token: str) -> dict:
    """Верификация JWT токена"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
) -> User:
    """Получение текущего пользователя по JWT"""
    token = credentials.credentials
    payload = verify_token(token)
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token payload")
    
    user = db.query(User).filter(User.id == int(user_id)).first()
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    
    return user

def get_user_by_username(db: Session, username: str) -> Optional[User]:
    """Получение пользователя по username"""
    return db.query(User).filter(User.username == username).first()

def get_user_by_hardware_id(db: Session, hardware_id: str) -> Optional[User]:
    """Получение пользователя по hardware_id"""
    return db.query(User).filter(User.hardware_id == hardware_id).first()

def is_user_blocked(db: Session, blocker_id: int, blocked_id: int) -> bool:
    """Проверка, заблокирован ли пользователь"""
    block = db.query(UserBlock).filter(
        UserBlock.blocker_id == blocker_id,
        UserBlock.blocked_id == blocked_id
    ).first()
    return block is not None

def can_access_chat(db: Session, user_id: int, chat_id: int) -> bool:
    """Проверка доступа пользователя к чату"""
    member = db.query(ChatMember).filter(
        ChatMember.chat_id == chat_id,
        ChatMember.user_id == user_id
    ).first()
    return member is not None

def can_send_message(db: Session, user_id: int, chat_id: int) -> bool:
    """Проверка, может ли пользователь отправлять сообщения в чат"""
    # Проверка доступа к чату
    if not can_access_chat(db, user_id, chat_id):
        return False
    
    # Проверка, не заблокирован ли пользователь в чате (для приватных чатов)
    chat = db.query(Chat).filter(Chat.id == chat_id).first()
    if not chat:
        return False
    
    # Для приватных чатов проверяем, не заблокировал ли получатель отправителя
    if chat.type == "private":
        # Находим второго участника
        members = db.query(ChatMember).filter(ChatMember.chat_id == chat_id).all()
        other_member = None
        for member in members:
            if member.user_id != user_id:
                other_member = member
                break
        
        if other_member:
            # Проверяем, не заблокировал ли другой пользователь текущего
            if is_user_blocked(db, other_member.user_id, user_id):
                return False
    
    return True

# WebSocket менеджер
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[int, WebSocket] = {}
        self.user_status: Dict[int, bool] = {}
    
    async def connect(self, websocket: WebSocket, user_id: int):
        """Подключение пользователя"""
        await websocket.accept()
        self.active_connections[user_id] = websocket
        self.user_status[user_id] = True
        logger.info(f"User {user_id} connected via WebSocket")
    
    def disconnect(self, user_id: int):
        """Отключение пользователя"""
        if user_id in self.active_connections:
            del self.active_connections[user_id]
        self.user_status[user_id] = False
        logger.info(f"User {user_id} disconnected from WebSocket")
    
    async def send_message(self, user_id: int, message: dict):
        """Отправка сообщения пользователю"""
        if user_id in self.active_connections:
            try:
                await self.active_connections[user_id].send_json(message)
                return True
            except:
                return False
        return False
    
    async def broadcast_status(self, user_id: int, is_online: bool):
        """Трансляция статуса пользователя"""
        status_message = {
            "type": "status",
            "user_id": user_id,
            "is_online": is_online
        }
        # Отправляем всем подключенным пользователям
        for conn_user_id, websocket in self.active_connections.items():
            if conn_user_id != user_id:
                try:
                    await websocket.send_json(status_message)
                except:
                    pass

manager = ConnectionManager()

# Фоновые задачи
def cleanup_temp_files():
    """Удаление временных файлов старше 15 минут"""
    logger.info("Running cleanup of temporary files")
    try:
        now = datetime.utcnow()
        for filename in os.listdir(UPLOAD_TEMP_DIR):
            filepath = os.path.join(UPLOAD_TEMP_DIR, filename)
            try:
                stat = os.stat(filepath)
                if stat.st_mtime < (now - timedelta(minutes=15)).timestamp():
                    os.remove(filepath)
                    logger.info(f"Deleted old temp file: {filename}")
            except Exception as e:
                logger.error(f"Error deleting file {filename}: {e}")
    except Exception as e:
        logger.error(f"Error in cleanup task: {e}")

def update_offline_status():
    """Обновление статуса офлайн для пользователей без активного WebSocket"""
    logger.info("Updating offline status")
    try:
        db = SessionLocal()
        try:
            # Пользователи, которые online, но не имеют активного WebSocket
            online_users = db.query(User).filter(User.is_online == True).all()
            for user in online_users:
                if user.id not in manager.active_connections:
                    user.is_online = False
                    user.last_seen = datetime.utcnow()
                    db.add(user)
                    # Транслируем статус
                    asyncio.create_task(manager.broadcast_status(user.id, False))
            db.commit()
        finally:
            db.close()
    except Exception as e:
        logger.error(f"Error updating offline status: {e}")

# Создание и запуск шедулера
scheduler = BackgroundScheduler()
scheduler.add_job(cleanup_temp_files, IntervalTrigger(minutes=5))
scheduler.add_job(update_offline_status, IntervalTrigger(minutes=2))
scheduler.start()

# FastAPI приложение
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan менеджер для управления жизненным циклом приложения"""
    logger.info("Starting ARTMessage backend...")
    yield
    logger.info("Shutting down ARTMessage backend...")
    scheduler.shutdown()

app = FastAPI(
    title="ARTMessage API",
    description="Messenger backend with E2EE",
    version="1.0.0",
    lifespan=lifespan
)

# CORS настройка
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000", "http://localhost:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# REST эндпоинты

# Auth endpoints
@app.post("/auth/register", response_model=dict)
async def register_user(
    username: str = Form(...),
    first_name: str = Form(...),
    last_name: Optional[str] = Form(None),
    bio: Optional[str] = Form(None),
    password: str = Form(...),
    public_key: str = Form(...),
    hardware_id: str = Form(...),
    avatar: Optional[UploadFile] = File(None),
    db: Session = Depends(get_db)
):
    """
    Регистрация нового пользователя
    
    Принимает данные пользователя и создает аккаунт.
    """
    try:
        # Проверка уникальности username
        if get_user_by_username(db, username):
            raise HTTPException(status_code=400, detail="Username already taken")
        
        # Проверка уникальности hardware_id
        if get_user_by_hardware_id(db, hardware_id):
            raise HTTPException(status_code=400, detail="Hardware ID already registered")
        
        # Хеширование пароля
        password_hash = get_password_hash(password)
        
        # Создание пользователя
        user = User(
            username=username,
            first_name=first_name,
            last_name=last_name,
            bio=bio,
            password_hash=password_hash,
            public_key=public_key,
            hardware_id=hardware_id,
            is_online=False,
            last_seen=datetime.utcnow()
        )
        
        db.add(user)
        db.commit()
        db.refresh(user)
        
        # Обработка аватара
        if avatar:
            try:
                # Создание директории для аватаров, если не существует
                os.makedirs(UPLOAD_AVATARS_DIR, exist_ok=True)
                
                # Сохранение аватара
                file_extension = os.path.splitext(avatar.filename)[1]
                avatar_filename = f"user_{user.id}_{uuid.uuid4().hex}{file_extension}"
                avatar_path = os.path.join(UPLOAD_AVATARS_DIR, avatar_filename)
                
                async with aiofiles.open(avatar_path, 'wb') as out_file:
                    content = await avatar.read()
                    await out_file.write(content)
                
                user.avatar_url = f"/files/avatars/{avatar_filename}"
                db.commit()
                
            except Exception as e:
                logger.error(f"Error saving avatar: {e}")
        
        logger.info(f"User registered: {username} (ID: {user.id})")
        return {"message": "User registered successfully", "user_id": user.id}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Registration error: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")

@app.post("/auth/login", response_model=Token)
async def login_user(user_data: UserLogin, db: Session = Depends(get_db)):
    """
    Вход пользователя
    
    Проверяет учетные данные и возвращает токены доступа.
    """
    try:
        user = get_user_by_username(db, user_data.username)
        if not user:
            raise HTTPException(status_code=401, detail="Invalid username or password")
        
        if not verify_password(user_data.password, user.password_hash):
            raise HTTPException(status_code=401, detail="Invalid username or password")
        
        # Обновление hardware_id при входе
        if user.hardware_id != user_data.hardware_id:
            # Проверяем, не занят ли hardware_id другим пользователем
            if get_user_by_hardware_id(db, user_data.hardware_id):
                raise HTTPException(status_code=400, detail="Hardware ID already in use")
            user.hardware_id = user_data.hardware_id
            db.commit()
        
        # Обновление статуса
        user.is_online = True
        user.last_seen = datetime.utcnow()
        db.commit()
        
        # Создание токенов
        access_token = create_access_token({"sub": str(user.id)})
        refresh_token = create_refresh_token({"sub": str(user.id)})
        
        # Трансляция статуса
        asyncio.create_task(manager.broadcast_status(user.id, True))
        
        logger.info(f"User logged in: {user.username} (ID: {user.id})")
        return {"access_token": access_token, "refresh_token": refresh_token}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Login error: {e}")
        raise HTTPException(status_code=500, detail=f"Login failed: {str(e)}")

@app.post("/auth/refresh", response_model=Token)
async def refresh_token(refresh_data: TokenRefresh, db: Session = Depends(get_db)):
    """
    Обновление токена доступа
    
    Использует refresh token для получения нового access token.
    """
    try:
        payload = verify_token(refresh_data.refresh_token)
        if payload.get("type") != "refresh":
            raise HTTPException(status_code=401, detail="Invalid token type")
        
        user_id = payload.get("sub")
        user = db.query(User).filter(User.id == int(user_id)).first()
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
        
        access_token = create_access_token({"sub": str(user.id)})
        refresh_token = create_refresh_token({"sub": str(user.id)})
        
        return {"access_token": access_token, "refresh_token": refresh_token}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Refresh token error: {e}")
        raise HTTPException(status_code=500, detail=f"Token refresh failed: {str(e)}")

@app.post("/auth/logout")
async def logout_user(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """
    Выход пользователя
    
    Обновляет статус пользователя на офлайн.
    """
    try:
        current_user.is_online = False
        current_user.last_seen = datetime.utcnow()
        db.commit()
        
        # Трансляция статуса
        asyncio.create_task(manager.broadcast_status(current_user.id, False))
        
        logger.info(f"User logged out: {current_user.username} (ID: {current_user.id})")
        return {"message": "Logged out successfully"}
        
    except Exception as e:
        logger.error(f"Logout error: {e}")
        raise HTTPException(status_code=500, detail=f"Logout failed: {str(e)}")

# User endpoints
@app.get("/user/me")
async def get_current_user_profile(current_user: User = Depends(get_current_user)):
    """
    Получение профиля текущего пользователя
    """
    try:
        return {
            "id": current_user.id,
            "username": current_user.username,
            "first_name": current_user.first_name,
            "last_name": current_user.last_name,
            "bio": current_user.bio,
            "avatar_url": current_user.avatar_url,
            "public_key": current_user.public_key,
            "is_online": current_user.is_online,
            "last_seen": current_user.last_seen.isoformat() if current_user.last_seen else None,
            "created_at": current_user.created_at.isoformat() if current_user.created_at else None
        }
    except Exception as e:
        logger.error(f"Get profile error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get profile: {str(e)}")

@app.put("/user/me")
async def update_user_profile(
    user_update: UserUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Обновление профиля пользователя
    """
    try:
        # Проверка уникальности username при изменении
        if user_update.username and user_update.username != current_user.username:
            if get_user_by_username(db, user_update.username):
                raise HTTPException(status_code=400, detail="Username already taken")
            current_user.username = user_update.username
        
        if user_update.first_name:
            current_user.first_name = user_update.first_name
        if user_update.last_name is not None:
            current_user.last_name = user_update.last_name
        if user_update.bio is not None:
            current_user.bio = user_update.bio
        
        current_user.updated_at = datetime.utcnow()
        db.commit()
        
        logger.info(f"Profile updated for user: {current_user.username} (ID: {current_user.id})")
        return {"message": "Profile updated successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update profile error: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Profile update failed: {str(e)}")

@app.put("/user/me/password")
async def change_password(
    password_data: PasswordChange,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Смена пароля пользователя
    """
    try:
        # Проверка старого пароля
        if not verify_password(password_data.old_password, current_user.password_hash):
            raise HTTPException(status_code=400, detail="Incorrect old password")
        
        # Установка нового пароля
        current_user.password_hash = get_password_hash(password_data.new_password)
        current_user.updated_at = datetime.utcnow()
        db.commit()
        
        logger.info(f"Password changed for user: {current_user.username} (ID: {current_user.id})")
        return {"message": "Password changed successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Change password error: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Password change failed: {str(e)}")

@app.post("/user/me/avatar")
async def upload_avatar(
    avatar: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Загрузка аватара пользователя
    """
    try:
        # Проверка типа файла
        if avatar.content_type not in ['image/jpeg', 'image/png', 'image/gif']:
            raise HTTPException(status_code=400, detail="Only JPEG, PNG, GIF images are allowed")
        
        # Проверка размера
        content = await avatar.read()
        if len(content) > 5 * 1024 * 1024:  # 5 MB
            raise HTTPException(status_code=400, detail="Avatar size exceeds 5 MB")
        
        # Удаление старого аватара
        if current_user.avatar_url:
            old_avatar_path = current_user.avatar_url.replace("/files/avatars/", "")
            old_avatar_full_path = os.path.join(UPLOAD_AVATARS_DIR, old_avatar_path)
            if os.path.exists(old_avatar_full_path):
                os.remove(old_avatar_full_path)
        
        # Сохранение нового аватара
        file_extension = os.path.splitext(avatar.filename)[1]
        avatar_filename = f"user_{current_user.id}_{uuid.uuid4().hex}{file_extension}"
        avatar_path = os.path.join(UPLOAD_AVATARS_DIR, avatar_filename)
        
        async with aiofiles.open(avatar_path, 'wb') as out_file:
            await out_file.write(content)
        
        current_user.avatar_url = f"/files/avatars/{avatar_filename}"
        current_user.updated_at = datetime.utcnow()
        db.commit()
        
        logger.info(f"Avatar uploaded for user: {current_user.username} (ID: {current_user.id})")
        return {"message": "Avatar uploaded successfully", "avatar_url": current_user.avatar_url}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload avatar error: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Avatar upload failed: {str(e)}")

@app.get("/user/search")
async def search_users(
    q: str = Query(..., min_length=1, max_length=50),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Поиск пользователей по username
    """
    try:
        # Поиск с автодополнением
        users = db.query(User).filter(
            User.username.ilike(f"{q}%"),
            User.id != current_user.id
        ).limit(10).all()
        
        return [{
            "id": user.id,
            "username": user.username,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "avatar_url": user.avatar_url,
            "is_online": user.is_online
        } for user in users]
        
    except Exception as e:
        logger.error(f"Search users error: {e}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

# Chat endpoints
@app.post("/chats")
async def create_chat(
    chat_data: ChatCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Создание нового чата
    """
    try:
        # Создание чата
        chat = Chat(
            type=chat_data.type,
            name=chat_data.name if chat_data.type != "private" else None,
            description=chat_data.description,
            created_by=current_user.id
        )
        db.add(chat)
        db.commit()
        db.refresh(chat)
        
        # Добавление создателя как участника
        creator_member = ChatMember(
            chat_id=chat.id,
            user_id=current_user.id,
            role="creator" if chat.type != "private" else "member"
        )
        db.add(creator_member)
        
        # Для приватного чата добавляем второго участника
        if chat.type == "private" and chat_data.user_ids:
            # Для приватного чата должен быть указан один пользователь
            if len(chat_data.user_ids) != 1:
                db.delete(chat)
                db.commit()
                raise HTTPException(status_code=400, detail="Private chat requires exactly one other user")
            
            other_user_id = chat_data.user_ids[0]
            # Проверка, что пользователь существует и не заблокирован
            other_user = db.query(User).filter(User.id == other_user_id).first()
            if not other_user:
                db.delete(chat)
                db.commit()
                raise HTTPException(status_code=404, detail="User not found")
            
            # Проверка блокировки
            if is_user_blocked(db, other_user_id, current_user.id) or is_user_blocked(db, current_user.id, other_user_id):
                db.delete(chat)
                db.commit()
                raise HTTPException(status_code=403, detail="User is blocked")
            
            # Проверка существования приватного чата между этими пользователями
            existing_chat = db.query(Chat).join(ChatMember).filter(
                Chat.type == "private",
                ChatMember.user_id.in_([current_user.id, other_user_id])
            ).group_by(Chat.id).having(func.count(ChatMember.user_id) == 2).first()
            
            if existing_chat:
                db.delete(chat)
                db.commit()
                return {"message": "Private chat already exists", "chat_id": existing_chat.id}
            
            member = ChatMember(
                chat_id=chat.id,
                user_id=other_user_id,
                role="member"
            )
            db.add(member)
        
        # Для группы добавляем указанных пользователей
        elif chat.type == "group" and chat_data.user_ids:
            for user_id in chat_data.user_ids:
                # Проверка, что пользователь не заблокирован
                if not is_user_blocked(db, user_id, current_user.id) and not is_user_blocked(db, current_user.id, user_id):
                    member = ChatMember(
                        chat_id=chat.id,
                        user_id=user_id,
                        role="member"
                    )
                    db.add(member)
        
        db.commit()
        
        logger.info(f"Chat created: {chat.id} by user {current_user.id}")
        return {"message": "Chat created successfully", "chat_id": chat.id}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create chat error: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Chat creation failed: {str(e)}")

@app.get("/chats")
async def get_user_chats(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Получение списка чатов пользователя с пагинацией
    """
    try:
        offset = (page - 1) * limit
        
        # Получение чатов пользователя
        chats = db.query(Chat).join(ChatMember).filter(
            ChatMember.user_id == current_user.id
        ).order_by(Chat.created_at.desc()).offset(offset).limit(limit).all()
        
        result = []
        for chat in chats:
            # Получение информации о чате
            members = db.query(ChatMember).filter(ChatMember.chat_id == chat.id).all()
            member_count = len(members)
            
            # Получение последнего сообщения
            last_message = db.query(Message).filter(
                Message.chat_id == chat.id
            ).order_by(Message.created_at.desc()).first()
            
            # Получение информации о текущем пользователе в чате
            current_member = db.query(ChatMember).filter(
                ChatMember.chat_id == chat.id,
                ChatMember.user_id == current_user.id
            ).first()
            
            # Для приватных чатов получаем имя другого участника
            chat_name = chat.name
            if chat.type == "private":
                other_member = None
                for member in members:
                    if member.user_id != current_user.id:
                        other_member = member
                        break
                if other_member:
                    other_user = db.query(User).filter(User.id == other_member.user_id).first()
                    if other_user:
                        chat_name = f"{other_user.first_name} {other_user.last_name or ''}".strip() or other_user.username
            
            result.append({
                "id": chat.id,
                "type": chat.type,
                "name": chat_name,
                "avatar_url": chat.avatar_url,
                "member_count": member_count,
                "last_message": {
                    "content": last_message.content if last_message else None,
                    "created_at": last_message.created_at.isoformat() if last_message else None,
                    "sender_id": last_message.sender_id if last_message else None
                } if last_message else None,
                "is_pinned": current_member.is_pinned if current_member else False,
                "folder": current_member.folder if current_member else None,
                "created_at": chat.created_at.isoformat()
            })
        
        return {
            "chats": result,
            "page": page,
            "limit": limit,
            "total": db.query(ChatMember).filter(ChatMember.user_id == current_user.id).count()
        }
        
    except Exception as e:
        logger.error(f"Get chats error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get chats: {str(e)}")

@app.get("/chats/{chat_id}")
async def get_chat_details(
    chat_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Получение деталей чата
    """
    try:
        # Проверка доступа
        if not can_access_chat(db, current_user.id, chat_id):
            raise HTTPException(status_code=403, detail="Access denied")
        
        chat = db.query(Chat).filter(Chat.id == chat_id).first()
        if not chat:
            raise HTTPException(status_code=404, detail="Chat not found")
        
        # Получение участников
        members = db.query(ChatMember).filter(ChatMember.chat_id == chat_id).all()
        member_list = []
        for member in members:
            user = db.query(User).filter(User.id == member.user_id).first()
            if user:
                member_list.append({
                    "user_id": user.id,
                    "username": user.username,
                    "first_name": user.first_name,
                    "last_name": user.last_name,
                    "avatar_url": user.avatar_url,
                    "role": member.role,
                    "is_online": user.is_online,
                    "joined_at": member.joined_at.isoformat()
                })
        
        # Получение информации о текущем пользователе
        current_member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        return {
            "id": chat.id,
            "type": chat.type,
            "name": chat.name,
            "description": chat.description,
            "avatar_url": chat.avatar_url,
            "created_by": chat.created_by,
            "created_at": chat.created_at.isoformat(),
            "members": member_list,
            "member_count": len(member_list),
            "current_user_role": current_member.role if current_member else None,
            "is_pinned": current_member.is_pinned if current_member else False,
            "folder": current_member.folder if current_member else None
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get chat details error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get chat details: {str(e)}")

@app.delete("/chats/{chat_id}")
async def delete_chat(
    chat_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Удаление чата (только для создателя или админа)
    """
    try:
        chat = db.query(Chat).filter(Chat.id == chat_id).first()
        if not chat:
            raise HTTPException(status_code=404, detail="Chat not found")
        
        # Проверка прав
        member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if not member or member.role not in ["creator", "admin"]:
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        
        # Удаление всех сообщений
        db.query(Message).filter(Message.chat_id == chat_id).delete()
        
        # Удаление всех участников
        db.query(ChatMember).filter(ChatMember.chat_id == chat_id).delete()
        
        # Удаление чата
        db.delete(chat)
        db.commit()
        
        logger.info(f"Chat {chat_id} deleted by user {current_user.id}")
        return {"message": "Chat deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete chat error: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to delete chat: {str(e)}")

@app.post("/chats/{chat_id}/pin")
async def toggle_pin_chat(
    chat_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Закрепление/открепление чата
    """
    try:
        member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if not member:
            raise HTTPException(status_code=403, detail="Access denied")
        
        member.is_pinned = not member.is_pinned
        db.commit()
        
        logger.info(f"Chat {chat_id} pin toggled by user {current_user.id}")
        return {"message": "Chat pin toggled", "is_pinned": member.is_pinned}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Pin chat error: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to toggle pin: {str(e)}")

@app.put("/chats/{chat_id}/folder")
async def move_chat_to_folder(
    chat_id: int,
    folder_data: FolderUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Перемещение чата в папку
    """
    try:
        member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if not member:
            raise HTTPException(status_code=403, detail="Access denied")
        
        member.folder = folder_data.folder
        db.commit()
        
        logger.info(f"Chat {chat_id} moved to folder '{folder_data.folder}' by user {current_user.id}")
        return {"message": "Chat moved to folder", "folder": member.folder}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Move chat to folder error: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to move chat: {str(e)}")

@app.post("/chats/{chat_id}/members")
async def add_chat_member(
    chat_id: int,
    member_data: MemberAdd,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Добавление участника в чат
    """
    try:
        chat = db.query(Chat).filter(Chat.id == chat_id).first()
        if not chat:
            raise HTTPException(status_code=404, detail="Chat not found")
        
        # Проверка прав (только создатель или админ могут добавлять участников)
        if chat.type == "private":
            raise HTTPException(status_code=400, detail="Cannot add members to private chat")
        
        current_member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if not current_member or current_member.role not in ["creator", "admin"]:
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        
        # Проверка, что пользователь существует
        user_to_add = db.query(User).filter(User.id == member_data.user_id).first()
        if not user_to_add:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Проверка, не заблокирован ли пользователь
        if is_user_blocked(db, user_to_add.id, current_user.id) or is_user_blocked(db, current_user.id, user_to_add.id):
            raise HTTPException(status_code=403, detail="User is blocked")
        
        # Проверка, не состоит ли уже в чате
        existing_member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == member_data.user_id
        ).first()
        
        if existing_member:
            raise HTTPException(status_code=400, detail="User already in chat")
        
        # Добавление участника
        new_member = ChatMember(
            chat_id=chat_id,
            user_id=member_data.user_id,
            role="member"
        )
        db.add(new_member)
        db.commit()
        
        logger.info(f"User {member_data.user_id} added to chat {chat_id} by user {current_user.id}")
        return {"message": "Member added successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Add chat member error: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to add member: {str(e)}")

@app.delete("/chats/{chat_id}/members/{user_id}")
async def remove_chat_member(
    chat_id: int,
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Удаление участника из чата
    """
    try:
        chat = db.query(Chat).filter(Chat.id == chat_id).first()
        if not chat:
            raise HTTPException(status_code=404, detail="Chat not found")
        
        if chat.type == "private":
            raise HTTPException(status_code=400, detail="Cannot remove members from private chat")
        
        current_member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if not current_member:
            raise HTTPException(status_code=403, detail="Access denied")
        
        # Нельзя удалить создателя
        if current_user.id == user_id and current_member.role == "creator":
            raise HTTPException(status_code=403, detail="Cannot remove creator")
        
        # Проверка прав для удаления других пользователей
        if current_user.id != user_id and current_member.role not in ["creator", "admin"]:
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        
        # Удаление участника
        member_to_remove = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == user_id
        ).first()
        
        if not member_to_remove:
            raise HTTPException(status_code=404, detail="Member not found")
        
        # Если удаляем создателя, то передаем создателя админу
        if member_to_remove.role == "creator":
            # Находим первого админа для передачи прав
            new_creator = db.query(ChatMember).filter(
                ChatMember.chat_id == chat_id,
                ChatMember.role == "admin"
            ).first()
            
            if new_creator:
                new_creator.role = "creator"
            else:
                # Если нет админов, делаем создателем текущего пользователя
                current_member.role = "creator"
        
        db.delete(member_to_remove)
        db.commit()
        
        logger.info(f"User {user_id} removed from chat {chat_id} by user {current_user.id}")
        return {"message": "Member removed successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Remove chat member error: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to remove member: {str(e)}")

@app.put("/chats/{chat_id}/members/{user_id}/role")
async def update_member_role(
    chat_id: int,
    user_id: int,
    role_data: MemberRole,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Изменение роли участника
    """
    try:
        chat = db.query(Chat).filter(Chat.id == chat_id).first()
        if not chat:
            raise HTTPException(status_code=404, detail="Chat not found")
        
        if chat.type == "private":
            raise HTTPException(status_code=400, detail="Cannot change roles in private chat")
        
        current_member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if not current_member or current_member.role not in ["creator", "admin"]:
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        
        member_to_update = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == user_id
        ).first()
        
        if not member_to_update:
            raise HTTPException(status_code=404, detail="Member not found")
        
        # Нельзя изменить роль создателя
        if member_to_update.role == "creator":
            raise HTTPException(status_code=403, detail="Cannot change creator role")
        
        # Если меняем на creator, то должен быть создателем
        if role_data.role == "creator" and current_member.role != "creator":
            raise HTTPException(status_code=403, detail="Only creator can assign creator role")
        
        member_to_update.role = role_data.role
        db.commit()
        
        logger.info(f"Role updated for user {user_id} in chat {chat_id} by user {current_user.id}")
        return {"message": "Role updated successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update member role error: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to update role: {str(e)}")

# Message endpoints
@app.get("/chats/{chat_id}/messages")
async def get_chat_messages(
    chat_id: int,
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Получение истории сообщений чата с пагинацией
    """
    try:
        # Проверка доступа
        if not can_access_chat(db, current_user.id, chat_id):
            raise HTTPException(status_code=403, detail="Access denied")
        
        offset = (page - 1) * limit
        
        messages = db.query(Message).filter(
            Message.chat_id == chat_id
        ).order_by(Message.created_at.desc()).offset(offset).limit(limit).all()
        
        # Получение информации о отправителях
        result = []
        for message in messages:
            sender = db.query(User).filter(User.id == message.sender_id).first()
            result.append({
                "id": message.id,
                "message_id": message.message_id,
                "send_id": message.send_id,
                "sender_id": message.sender_id,
                "sender_username": sender.username if sender else None,
                "sender_first_name": sender.first_name if sender else None,
                "sender_last_name": sender.last_name if sender else None,
                "content": message.content,  # Зашифрованный текст
                "file_url": message.file_url,
                "file_type": message.file_type,
                "file_name": message.file_name,
                "is_read": message.is_read,
                "read_at": message.read_at.isoformat() if message.read_at else None,
                "delivered_at": message.delivered_at.isoformat() if message.delivered_at else None,
                "created_at": message.created_at.isoformat()
            })
        
        return {
            "messages": result,
            "page": page,
            "limit": limit,
            "total": db.query(Message).filter(Message.chat_id == chat_id).count()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get messages error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get messages: {str(e)}")

@app.post("/chats/{chat_id}/messages")
async def send_message(
    chat_id: int,
    message_data: MessageCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Отправка сообщения в чат
    """
    try:
        # Проверка прав на отправку сообщения
        if not can_send_message(db, current_user.id, chat_id):
            raise HTTPException(status_code=403, detail="Cannot send message")
        
        # Проверка уникальности message_id
        existing_message = db.query(Message).filter(
            Message.message_id == message_data.message_id
        ).first()
        
        if existing_message:
            raise HTTPException(status_code=400, detail="Message ID already exists")
        
        # Создание сообщения
        message = Message(
            message_id=message_data.message_id,
            send_id=message_data.send_id,
            chat_id=chat_id,
            sender_id=current_user.id,
            content=message_data.content,
            file_url=message_data.file_url
        )
        
        db.add(message)
        db.commit()
        db.refresh(message)
        
        # Получение отправителя
        sender = db.query(User).filter(User.id == current_user.id).first()
        
        # Отправка сообщения через WebSocket всем участникам чата
        members = db.query(ChatMember).filter(ChatMember.chat_id == chat_id).all()
        for member in members:
            if member.user_id != current_user.id:  # Не отправляем отправителю
                # Проверка, не заблокирован ли отправитель получателем
                if not is_user_blocked(db, member.user_id, current_user.id):
                    ws_message = {
                        "type": "message",
                        "message_id": message.message_id,
                        "send_id": message.send_id,
                        "sender_id": message.sender_id,
                        "chat_id": message.chat_id,
                        "content": message.content,
                        "file_url": message.file_url,
                        "created_at": message.created_at.isoformat()
                    }
                    await manager.send_message(member.user_id, ws_message)
        
        logger.info(f"Message sent: {message.message_id} by user {current_user.id} in chat {chat_id}")
        return {
            "message": "Message sent successfully",
            "message_id": message.message_id,
            "send_id": message.send_id,
            "created_at": message.created_at.isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Send message error: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to send message: {str(e)}")

@app.put("/messages/{message_id}/read")
async def mark_message_read(
    message_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Отметка сообщения как прочитанного
    """
    try:
        message = db.query(Message).filter(Message.message_id == message_id).first()
        if not message:
            raise HTTPException(status_code=404, detail="Message not found")
        
        # Проверка доступа к чату
        if not can_access_chat(db, current_user.id, message.chat_id):
            raise HTTPException(status_code=403, detail="Access denied")
        
        # Нельзя отметить свое сообщение как прочитанное
        if message.sender_id == current_user.id:
            raise HTTPException(status_code=400, detail="Cannot mark own message as read")
        
        message.is_read = True
        message.read_at = datetime.utcnow()
        db.commit()
        
        # Отправка подтверждения через WebSocket
        ws_message = {
            "type": "read",
            "message_id": message.message_id,
            "send_id": message.send_id
        }
        await manager.send_message(message.sender_id, ws_message)
        
        logger.info(f"Message {message_id} marked as read by user {current_user.id}")
        return {"message": "Message marked as read"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Mark message read error: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to mark message as read: {str(e)}")

@app.get("/messages/unread")
async def get_unread_count(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Получение количества непрочитанных сообщений
    """
    try:
        # Получение всех чатов пользователя
        chat_ids = db.query(ChatMember.chat_id).filter(
            ChatMember.user_id == current_user.id
        ).all()
        chat_ids = [chat_id[0] for chat_id in chat_ids]
        
        # Подсчет непрочитанных сообщений
        unread_count = db.query(Message).filter(
            Message.chat_id.in_(chat_ids),
            Message.sender_id != current_user.id,
            Message.is_read == False
        ).count()
        
        return {"unread_count": unread_count}
        
    except Exception as e:
        logger.error(f"Get unread count error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get unread count: {str(e)}")

# File endpoints
@app.post("/upload/temp")
async def upload_temp_file(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Загрузка временного файла (до 500 KB)
    """
    try:
        # Проверка типа файла
        if file.content_type not in ALLOWED_FILE_TYPES:
            raise HTTPException(status_code=400, detail="File type not allowed")
        
        # Проверка размера
        content = await file.read()
        if len(content) > MAX_FILE_SIZE:
            raise HTTPException(status_code=400, detail=f"File size exceeds {MAX_FILE_SIZE // 1024} KB")
        
        # Генерация имени файла
        file_extension = os.path.splitext(file.filename)[1]
        temp_filename = f"temp_{uuid.uuid4().hex}{file_extension}"
        temp_path = os.path.join(UPLOAD_TEMP_DIR, temp_filename)
        
        # Сохранение файла
        async with aiofiles.open(temp_path, 'wb') as out_file:
            await out_file.write(content)
        
        # Определение типа файла
        file_type = "image" if file.content_type.startswith("image/") else "document"
        
        logger.info(f"Temp file uploaded: {temp_filename} by user {current_user.id}")
        return {
            "message": "File uploaded successfully",
            "file_url": f"/files/temp/{temp_filename}",
            "file_type": file_type,
            "file_name": file.filename,
            "file_size": len(content)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload temp file error: {e}")
        raise HTTPException(status_code=500, detail=f"File upload failed: {str(e)}")

@app.get("/files/{file_path:path}")
async def get_file(
    file_path: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Скачивание файла с проверкой прав доступа
    """
    try:
        # Разделяем путь на тип и имя файла
        parts = file_path.split('/')
        if len(parts) < 2:
            raise HTTPException(status_code=400, detail="Invalid file path")
        
        file_type = parts[0]
        filename = '/'.join(parts[1:])
        
        if file_type == "avatars":
            # Аватары доступны всем
            full_path = os.path.join(UPLOAD_AVATARS_DIR, filename)
            if not os.path.exists(full_path):
                raise HTTPException(status_code=404, detail="File not found")
            return FileResponse(full_path)
            
        elif file_type == "temp":
            # Временные файлы требуют проверки доступа
            full_path = os.path.join(UPLOAD_TEMP_DIR, filename)
            if not os.path.exists(full_path):
                raise HTTPException(status_code=404, detail="File not found")
            
            # Проверка, имеет ли пользователь доступ к этому файлу
            # Ищем сообщение с этим файлом
            message = db.query(Message).filter(
                Message.file_url.like(f"%{filename}")
            ).first()
            
            if not message:
                raise HTTPException(status_code=403, detail="Access denied")
            
            # Проверяем, является ли пользователь участником чата
            if not can_access_chat(db, current_user.id, message.chat_id):
                raise HTTPException(status_code=403, detail="Access denied")
            
            return FileResponse(full_path)
        else:
            raise HTTPException(status_code=400, detail="Invalid file type")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get file error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get file: {str(e)}")

# Block endpoints
@app.post("/blocks/{user_id}")
async def block_user(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Блокировка пользователя
    """
    try:
        if user_id == current_user.id:
            raise HTTPException(status_code=400, detail="Cannot block yourself")
        
        user_to_block = db.query(User).filter(User.id == user_id).first()
        if not user_to_block:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Проверка существующей блокировки
        existing_block = db.query(UserBlock).filter(
            UserBlock.blocker_id == current_user.id,
            UserBlock.blocked_id == user_id
        ).first()
        
        if existing_block:
            raise HTTPException(status_code=400, detail="User already blocked")
        
        # Создание блокировки
        block = UserBlock(
            blocker_id=current_user.id,
            blocked_id=user_id
        )
        db.add(block)
        db.commit()
        
        # Удаление из чатов, если есть
        # Находим общие чаты
        user_chats = db.query(ChatMember.chat_id).filter(
            ChatMember.user_id == current_user.id
        ).all()
        user_chat_ids = [chat[0] for chat in user_chats]
        
        blocked_chats = db.query(ChatMember.chat_id).filter(
            ChatMember.user_id == user_id,
            ChatMember.chat_id.in_(user_chat_ids)
        ).all()
        blocked_chat_ids = [chat[0] for chat in blocked_chats]
        
        # Удаляем из общих чатов
        for chat_id in blocked_chat_ids:
            chat = db.query(Chat).filter(Chat.id == chat_id).first()
            if chat and chat.type == "private":
                # Удаляем приватный чат
                db.query(Message).filter(Message.chat_id == chat_id).delete()
                db.query(ChatMember).filter(ChatMember.chat_id == chat_id).delete()
                db.delete(chat)
            else:
                # Удаляем из группового чата
                db.query(ChatMember).filter(
                    ChatMember.chat_id == chat_id,
                    ChatMember.user_id == user_id
                ).delete()
        
        db.commit()
        
        logger.info(f"User {user_id} blocked by user {current_user.id}")
        return {"message": "User blocked successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Block user error: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to block user: {str(e)}")

@app.delete("/blocks/{user_id}")
async def unblock_user(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Разблокировка пользователя
    """
    try:
        block = db.query(UserBlock).filter(
            UserBlock.blocker_id == current_user.id,
            UserBlock.blocked_id == user_id
        ).first()
        
        if not block:
            raise HTTPException(status_code=404, detail="User not blocked")
        
        db.delete(block)
        db.commit()
        
        logger.info(f"User {user_id} unblocked by user {current_user.id}")
        return {"message": "User unblocked successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unblock user error: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to unblock user: {str(e)}")

@app.get("/blocks")
async def get_blocked_users(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Получение списка заблокированных пользователей
    """
    try:
        blocks = db.query(UserBlock).filter(
            UserBlock.blocker_id == current_user.id
        ).all()
        
        result = []
        for block in blocks:
            user = db.query(User).filter(User.id == block.blocked_id).first()
            if user:
                result.append({
                    "id": user.id,
                    "username": user.username,
                    "first_name": user.first_name,
                    "last_name": user.last_name,
                    "avatar_url": user.avatar_url,
                    "blocked_at": block.created_at.isoformat()
                })
        
        return {"blocked_users": result}
        
    except Exception as e:
        logger.error(f"Get blocked users error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get blocked users: {str(e)}")

# Channel endpoints
@app.post("/channels/{chat_id}/subscribe")
async def subscribe_to_channel(
    chat_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Подписка/отписка на канал
    """
    try:
        chat = db.query(Chat).filter(
            Chat.id == chat_id,
            Chat.type == "channel"
        ).first()
        
        if not chat:
            raise HTTPException(status_code=404, detail="Channel not found")
        
        # Проверка, не заблокирован ли пользователь
        if is_user_blocked(db, chat.created_by, current_user.id):
            raise HTTPException(status_code=403, detail="You are blocked by the channel creator")
        
        # Проверка существующей подписки
        existing_member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if existing_member:
            # Отписка
            db.delete(existing_member)
            db.commit()
            logger.info(f"User {current_user.id} unsubscribed from channel {chat_id}")
            return {"message": "Unsubscribed from channel"}
        else:
            # Подписка
            new_member = ChatMember(
                chat_id=chat_id,
                user_id=current_user.id,
                role="member"
            )
            db.add(new_member)
            db.commit()
            logger.info(f"User {current_user.id} subscribed to channel {chat_id}")
            return {"message": "Subscribed to channel"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Channel subscribe error: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to subscribe: {str(e)}")

@app.get("/channels")
async def get_public_channels(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Получение списка публичных каналов
    """
    try:
        offset = (page - 1) * limit
        
        channels = db.query(Chat).filter(
            Chat.type == "channel"
        ).order_by(Chat.created_at.desc()).offset(offset).limit(limit).all()
        
        result = []
        for channel in channels:
            # Получение количества подписчиков
            member_count = db.query(ChatMember).filter(
                ChatMember.chat_id == channel.id
            ).count()
            
            # Проверка, подписан ли пользователь
            is_subscribed = db.query(ChatMember).filter(
                ChatMember.chat_id == channel.id,
                ChatMember.user_id == current_user.id
            ).first() is not None
            
            creator = db.query(User).filter(User.id == channel.created_by).first()
            
            result.append({
                "id": channel.id,
                "name": channel.name,
                "description": channel.description,
                "avatar_url": channel.avatar_url,
                "creator": {
                    "id": creator.id if creator else None,
                    "username": creator.username if creator else None,
                    "first_name": creator.first_name if creator else None
                } if creator else None,
                "member_count": member_count,
                "is_subscribed": is_subscribed,
                "created_at": channel.created_at.isoformat()
            })
        
        return {
            "channels": result,
            "page": page,
            "limit": limit,
            "total": db.query(Chat).filter(Chat.type == "channel").count()
        }
        
    except Exception as e:
        logger.error(f"Get channels error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get channels: {str(e)}")

# WebSocket endpoint
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket эндпоинт для живых сообщений
    """
    token = websocket.query_params.get("token")
    if not token:
        await websocket.close(code=1008, reason="Token required")
        return
    
    try:
        # Верификация токена
        payload = verify_token(token)
        user_id = int(payload.get("sub"))
        
        # Получение пользователя из БД
        db = SessionLocal()
        try:
            user = db.query(User).filter(User.id == user_id).first()
            if not user:
                await websocket.close(code=1008, reason="User not found")
                return
            
            # Подключение WebSocket
            await manager.connect(websocket, user_id)
            
            # Обновление статуса
            user.is_online = True
            user.last_seen = datetime.utcnow()
            db.commit()
            
            # Трансляция статуса онлайн
            await manager.broadcast_status(user_id, True)
            
            # Обработка сообщений
            try:
                while True:
                    data = await websocket.receive_text()
                    try:
                        message_data = json.loads(data)
                        msg_type = message_data.get("type")
                        
                        if msg_type == "message":
                            # Обработка отправленного сообщения
                            chat_id = message_data.get("chat_id")
                            message_id = message_data.get("message_id")
                            send_id = message_data.get("send_id")
                            content = message_data.get("content")
                            file_url = message_data.get("file_url")
                            
                            if not all([chat_id, message_id, send_id, content]):
                                await websocket.send_json({
                                    "type": "error",
                                    "message": "Missing required fields"
                                })
                                continue
                            
                            # Проверка прав на отправку
                            if not can_send_message(db, user_id, chat_id):
                                await websocket.send_json({
                                    "type": "error",
                                    "message": "Cannot send message"
                                })
                                continue
                            
                            # Проверка уникальности message_id
                            existing_message = db.query(Message).filter(
                                Message.message_id == message_id
                            ).first()
                            
                            if existing_message:
                                await websocket.send_json({
                                    "type": "error",
                                    "message": "Message ID already exists"
                                })
                                continue
                            
                            # Создание сообщения в БД
                            message = Message(
                                message_id=message_id,
                                send_id=send_id,
                                chat_id=chat_id,
                                sender_id=user_id,
                                content=content,
                                file_url=file_url
                            )
                            db.add(message)
                            db.commit()
                            db.refresh(message)
                            
                            # Отправка сообщения всем участникам чата
                            members = db.query(ChatMember).filter(
                                ChatMember.chat_id == chat_id
                            ).all()
                            
                            ws_message = {
                                "type": "message",
                                "message_id": message.message_id,
                                "send_id": message.send_id,
                                "sender_id": message.sender_id,
                                "chat_id": message.chat_id,
                                "content": message.content,
                                "file_url": message.file_url,
                                "created_at": message.created_at.isoformat()
                            }
                            
                            for member in members:
                                if member.user_id != user_id:  # Не отправляем отправителю
                                    # Проверка блокировки
                                    if not is_user_blocked(db, member.user_id, user_id):
                                        await manager.send_message(member.user_id, ws_message)
                            
                            # Подтверждение отправки отправителю
                            await websocket.send_json({
                                "type": "delivered",
                                "message_id": message.message_id,
                                "send_id": message.send_id
                            })
                            
                        elif msg_type == "delivered":
                            # Подтверждение доставки сообщения
                            message_id = message_data.get("message_id")
                            send_id = message_data.get("send_id")
                            
                            if not all([message_id, send_id]):
                                await websocket.send_json({
                                    "type": "error",
                                    "message": "Missing fields"
                                })
                                continue
                            
                            # Обновление статуса доставки
                            message = db.query(Message).filter(
                                Message.message_id == message_id,
                                Message.send_id == send_id
                            ).first()
                            
                            if message:
                                message.delivered_at = datetime.utcnow()
                                db.commit()
                                
                                # Уведомление отправителя
                                await manager.send_message(message.sender_id, {
                                    "type": "delivered",
                                    "message_id": message.message_id,
                                    "send_id": message.send_id
                                })
                            
                        elif msg_type == "read":
                            # Подтверждение прочтения
                            message_id = message_data.get("message_id")
                            send_id = message_data.get("send_id")
                            
                            if not all([message_id, send_id]):
                                await websocket.send_json({
                                    "type": "error",
                                    "message": "Missing fields"
                                })
                                continue
                            
                            # Обновление статуса прочтения
                            message = db.query(Message).filter(
                                Message.message_id == message_id,
                                Message.send_id == send_id
                            ).first()
                            
                            if message and message.sender_id != user_id:
                                message.is_read = True
                                message.read_at = datetime.utcnow()
                                db.commit()
                                
                                # Уведомление отправителя
                                await manager.send_message(message.sender_id, {
                                    "type": "read",
                                    "message_id": message.message_id,
                                    "send_id": message.send_id
                                })
                        
                        else:
                            await websocket.send_json({
                                "type": "error",
                                "message": f"Unknown message type: {msg_type}"
                            })
                            
                    except json.JSONDecodeError:
                        await websocket.send_json({
                            "type": "error",
                            "message": "Invalid JSON"
                        })
                    except Exception as e:
                        logger.error(f"WebSocket message processing error: {e}")
                        await websocket.send_json({
                            "type": "error",
                            "message": f"Processing error: {str(e)}"
                        })
                        
            except WebSocketDisconnect:
                logger.info(f"WebSocket disconnected for user {user_id}")
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
            finally:
                # Отключение пользователя
                manager.disconnect(user_id)
                
                # Обновление статуса в БД
                user.is_online = False
                user.last_seen = datetime.utcnow()
                db.commit()
                
                # Трансляция статуса офлайн
                await manager.broadcast_status(user_id, False)
                
        finally:
            db.close()
            
    except HTTPException as e:
        await websocket.close(code=1008, reason=str(e.detail))
    except Exception as e:
        logger.error(f"WebSocket connection error: {e}")
        await websocket.close(code=1011, reason="Internal server error")

# Запуск приложения
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
