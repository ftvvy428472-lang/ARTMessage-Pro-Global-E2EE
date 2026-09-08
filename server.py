# -*- coding: utf-8 -*-
"""
ARTMessage Backend Server
Полноценный бэкенд для мессенджера с E2EE шифрованием
"""

import asyncio
import os
import uuid
import shutil
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from pathlib import Path

from fastapi import FastAPI, HTTPException, Depends, status, UploadFile, File, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field, validator
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, ForeignKey, Text, Index, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from sqlalchemy.sql import func
from sqlalchemy.exc import IntegrityError
from passlib.context import CryptContext
from jose import JWTError, jwt
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import bcrypt

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Конфигурация
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 15
REFRESH_TOKEN_EXPIRE_DAYS = 7
UPLOAD_DIR = Path("uploads")
TEMP_DIR = UPLOAD_DIR / "temp"
AVATAR_DIR = UPLOAD_DIR / "avatars"
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///database.db")
MAX_FILE_SIZE = 500 * 1024  # 500 KB

# Создание директорий
UPLOAD_DIR.mkdir(exist_ok=True)
TEMP_DIR.mkdir(exist_ok=True, parents=True)
AVATAR_DIR.mkdir(exist_ok=True, parents=True)

# Инициализация FastAPI
app = FastAPI(
    title="ARTMessage API",
    description="Мессенджер с сквозным шифрованием",
    version="1.0.0"
)

# CORS настройки
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# База данных
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Security
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer()

# WebSocket менеджер
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[int, WebSocket] = {}
        self.connection_sids: Dict[WebSocket, str] = {}
    
    async def connect(self, websocket: WebSocket, user_id: int):
        await websocket.accept()
        self.active_connections[user_id] = websocket
        self.connection_sids[websocket] = str(uuid.uuid4())
        logger.info(f"User {user_id} connected via WebSocket")
    
    def disconnect(self, websocket: WebSocket):
        user_id = None
        for uid, ws in self.active_connections.items():
            if ws == websocket:
                user_id = uid
                break
        if user_id:
            del self.active_connections[user_id]
        if websocket in self.connection_sids:
            del self.connection_sids[websocket]
        logger.info(f"User {user_id} disconnected from WebSocket")
        return user_id
    
    async def send_message(self, user_id: int, message: dict):
        if user_id in self.active_connections:
            try:
                await self.active_connections[user_id].send_json(message)
                return True
            except Exception as e:
                logger.error(f"Error sending message to user {user_id}: {e}")
                return False
        return False

manager = ConnectionManager()

# ======================== МОДЕЛИ БД ========================

class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=True)
    bio = Column(Text, nullable=True)
    password_hash = Column(String(255), nullable=False)
    avatar_url = Column(String(255), nullable=True)
    public_key = Column(Text, nullable=False)
    hardware_id = Column(String(255), unique=True, nullable=False)
    is_online = Column(Boolean, default=False)
    last_seen = Column(DateTime, default=func.now())
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Отношения
    sent_messages = relationship("Message", foreign_keys="Message.sender_id", back_populates="sender")
    chat_memberships = relationship("ChatMember", back_populates="user")
    blocks = relationship("UserBlock", foreign_keys="UserBlock.blocker_id", back_populates="blocker")
    blocked_by = relationship("UserBlock", foreign_keys="UserBlock.blocked_id", back_populates="blocked")

class Chat(Base):
    __tablename__ = "chats"
    
    id = Column(Integer, primary_key=True, index=True)
    type = Column(String(20), nullable=False)  # private, group, channel
    name = Column(String(100), nullable=True)
    description = Column(Text, nullable=True)
    avatar_url = Column(String(255), nullable=True)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime, default=func.now())
    
    # Отношения
    creator = relationship("User", foreign_keys=[created_by])
    members = relationship("ChatMember", back_populates="chat")
    messages = relationship("Message", back_populates="chat")

class ChatMember(Base):
    __tablename__ = "chat_members"
    
    id = Column(Integer, primary_key=True, index=True)
    chat_id = Column(Integer, ForeignKey("chats.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    role = Column(String(20), nullable=False, default="member")  # creator, admin, member
    is_pinned = Column(Boolean, default=False)
    folder = Column(String(50), nullable=True)
    joined_at = Column(DateTime, default=func.now())
    
    # Отношения
    chat = relationship("Chat", back_populates="members")
    user = relationship("User", back_populates="chat_memberships")
    
    __table_args__ = (
        UniqueConstraint('chat_id', 'user_id', name='unique_chat_user'),
    )

class Message(Base):
    __tablename__ = "messages"
    
    id = Column(Integer, primary_key=True, index=True)
    message_id = Column(String(36), unique=True, nullable=False)
    send_id = Column(String(36), nullable=False)
    chat_id = Column(Integer, ForeignKey("chats.id"), nullable=False)
    sender_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    content = Column(Text, nullable=False)
    file_url = Column(String(255), nullable=True)
    file_type = Column(String(20), nullable=True)  # image, document
    file_name = Column(String(255), nullable=True)
    is_read = Column(Boolean, default=False)
    read_at = Column(DateTime, nullable=True)
    delivered_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=func.now())
    
    # Отношения
    chat = relationship("Chat", back_populates="messages")
    sender = relationship("User", foreign_keys=[sender_id], back_populates="sent_messages")
    
    __table_args__ = (
        Index('idx_message_chat_id_created_at', 'chat_id', 'created_at'),
        UniqueConstraint('message_id', 'send_id', name='unique_message_send'),
    )

class UserBlock(Base):
    __tablename__ = "user_blocks"
    
    id = Column(Integer, primary_key=True, index=True)
    blocker_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    blocked_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime, default=func.now())
    
    # Отношения
    blocker = relationship("User", foreign_keys=[blocker_id], back_populates="blocks")
    blocked = relationship("User", foreign_keys=[blocked_id], back_populates="blocked_by")
    
    __table_args__ = (
        UniqueConstraint('blocker_id', 'blocked_id', name='unique_block_pair'),
    )

# Создание таблиц
Base.metadata.create_all(bind=engine)

# ======================== PYDANTIC СХЕМЫ ========================

class UserCreate(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    first_name: str = Field(..., min_length=1, max_length=100)
    last_name: Optional[str] = Field(None, max_length=100)
    bio: Optional[str] = Field(None, max_length=500)
    password: str = Field(..., min_length=8)
    public_key: str
    hardware_id: str = Field(..., min_length=10)
    
    @validator('username')
    def validate_username(cls, v):
        if not v.isalnum() and '_' not in v:
            raise ValueError('Username должен содержать только буквы, цифры и подчеркивания')
        return v.lower()

class UserLogin(BaseModel):
    username: str
    password: str
    hardware_id: str

class RefreshToken(BaseModel):
    refresh_token: str

class UserUpdate(BaseModel):
    username: Optional[str] = Field(None, min_length=3, max_length=50)
    first_name: Optional[str] = Field(None, min_length=1, max_length=100)
    last_name: Optional[str] = Field(None, max_length=100)
    bio: Optional[str] = Field(None, max_length=500)

class PasswordChange(BaseModel):
    old_password: str
    new_password: str = Field(..., min_length=8)

# ИСПРАВЛЕНО: regex -> pattern
class ChatCreate(BaseModel):
    type: str = Field(..., pattern="^(private|group|channel)$")
    name: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = Field(None, max_length=500)
    members: Optional[List[int]] = []  # IDs пользователей для добавления

class ChatUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = Field(None, max_length=500)

class MessageCreate(BaseModel):
    content: str
    message_id: str
    send_id: str
    file_url: Optional[str] = None

class MessageResponse(BaseModel):
    message_id: str
    send_id: str
    sender_id: int
    chat_id: int
    content: str
    file_url: Optional[str]
    created_at: datetime

class FileUploadResponse(BaseModel):
    file_id: str
    url: str

class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"

# ======================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ========================

def get_db():
    """Получение сессии базы данных"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def hash_password(password: str) -> str:
    """Хеширование пароля"""
    return pwd_context.hash(password)

def verify_password(password: str, hashed: str) -> bool:
    """Проверка пароля"""
    return pwd_context.verify(password, hashed)

def create_access_token(data: dict) -> str:
    """Создание access токена"""
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire, "type": "access"})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def create_refresh_token(data: dict) -> str:
    """Создание refresh токена"""
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire, "type": "refresh"})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def verify_token(token: str) -> dict:
    """Верификация токена"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Недействительный токен",
            headers={"WWW-Authenticate": "Bearer"},
        )

def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
) -> User:
    """Получение текущего пользователя из токена"""
    token = credentials.credentials
    payload = verify_token(token)
    user_id = payload.get("sub")
    if user_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Недействительный токен"
        )
    
    user = db.query(User).filter(User.id == int(user_id)).first()
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Пользователь не найден"
        )
    
    return user

def save_upload_file(upload_file: UploadFile, directory: Path, filename: str) -> str:
    """Сохранение загруженного файла"""
    file_path = directory / filename
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(upload_file.file, buffer)
    return str(file_path)

def generate_file_id() -> str:
    """Генерация уникального ID для файла"""
    return str(uuid.uuid4())

def is_user_blocked(db: Session, user_id: int, target_id: int) -> bool:
    """Проверка, заблокирован ли пользователь"""
    block = db.query(UserBlock).filter(
        UserBlock.blocker_id == target_id,
        UserBlock.blocked_id == user_id
    ).first()
    return block is not None

async def send_online_status(user_id: int, is_online: bool):
    """Отправка статуса онлайна всем пользователям"""
    status_message = {
        "type": "status",
        "user_id": user_id,
        "is_online": is_online
    }
    for ws in manager.active_connections.values():
        try:
            await ws.send_json(status_message)
        except:
            pass

# ======================== ФОНОВЫЕ ЗАДАЧИ ========================

async def cleanup_temp_files():
    """Удаление временных файлов старше 15 минут"""
    try:
        logger.info("Запуск очистки временных файлов...")
        now = datetime.now()
        for file_path in TEMP_DIR.glob("*"):
            if file_path.is_file():
                file_age = now - datetime.fromtimestamp(file_path.stat().st_mtime)
                if file_age > timedelta(minutes=15):
                    file_path.unlink()
                    logger.info(f"Удалён временный файл: {file_path}")
    except Exception as e:
        logger.error(f"Ошибка при очистке временных файлов: {e}")

def setup_scheduler():
    """Настройка планировщика"""
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        cleanup_temp_files,
        trigger=IntervalTrigger(minutes=5),
        id="cleanup_temp_files"
    )
    scheduler.start()
    logger.info("Планировщик фоновых задач запущен")

# Запуск планировщика при старте
@app.on_event("startup")
async def startup_event():
    setup_scheduler()
    logger.info("Сервер ARTMessage запущен!")

# ======================== REST ЭНДПОИНТЫ ========================

# --- АУТЕНТИФИКАЦИЯ ---

@app.post("/auth/register", response_model=TokenResponse)
async def register(user_data: UserCreate, db: Session = Depends(get_db)):
    """Регистрация нового пользователя"""
    try:
        # Проверка существования пользователя
        existing_user = db.query(User).filter(
            (User.username == user_data.username) |
            (User.hardware_id == user_data.hardware_id)
        ).first()
        
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Пользователь с таким username или hardware_id уже существует"
            )
        
        # Создание пользователя
        new_user = User(
            username=user_data.username,
            first_name=user_data.first_name,
            last_name=user_data.last_name,
            bio=user_data.bio,
            password_hash=hash_password(user_data.password),
            public_key=user_data.public_key,
            hardware_id=user_data.hardware_id
        )
        
        db.add(new_user)
        db.commit()
        db.refresh(new_user)
        
        # Создание токенов
        access_token = create_access_token({"sub": str(new_user.id)})
        refresh_token = create_refresh_token({"sub": str(new_user.id)})
        
        logger.info(f"Зарегистрирован новый пользователь: {new_user.username} (ID: {new_user.id})")
        
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer"
        }
        
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Ошибка при создании пользователя"
        )
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка при регистрации: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

@app.post("/auth/login", response_model=TokenResponse)
async def login(user_data: UserLogin, db: Session = Depends(get_db)):
    """Вход пользователя"""
    try:
        user = db.query(User).filter(User.username == user_data.username).first()
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Неверный username или пароль"
            )
        
        if not verify_password(user_data.password, user.password_hash):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Неверный username или пароль"
            )
        
        # Обновление hardware_id если изменился
        if user.hardware_id != user_data.hardware_id:
            user.hardware_id = user_data.hardware_id
            db.commit()
        
        # Обновление статуса онлайн
        user.is_online = True
        user.last_seen = datetime.utcnow()
        db.commit()
        
        # Создание токенов
        access_token = create_access_token({"sub": str(user.id)})
        refresh_token = create_refresh_token({"sub": str(user.id)})
        
        logger.info(f"Пользователь {user.username} вошёл в систему")
        
        # Отправка статуса онлайн
        await send_online_status(user.id, True)
        
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при входе: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

@app.post("/auth/refresh", response_model=TokenResponse)
async def refresh_token(refresh_data: RefreshToken, db: Session = Depends(get_db)):
    """Обновление access токена"""
    try:
        payload = verify_token(refresh_data.refresh_token)
        
        if payload.get("type") != "refresh":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Недействительный refresh токен"
            )
        
        user_id = payload.get("sub")
        user = db.query(User).filter(User.id == int(user_id)).first()
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Пользователь не найден"
            )
        
        new_access_token = create_access_token({"sub": str(user.id)})
        
        return {
            "access_token": new_access_token,
            "refresh_token": refresh_data.refresh_token,
            "token_type": "bearer"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при обновлении токена: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

@app.post("/auth/logout")
async def logout(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Выход из системы"""
    try:
        current_user.is_online = False
        current_user.last_seen = datetime.utcnow()
        db.commit()
        
        logger.info(f"Пользователь {current_user.username} вышел из системы")
        
        # Отправка статуса офлайн
        await send_online_status(current_user.id, False)
        
        return {"message": "Выход выполнен успешно"}
        
    except Exception as e:
        logger.error(f"Ошибка при выходе: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

# --- ПРОФИЛЬ ---

@app.get("/user/me")
async def get_profile(current_user: User = Depends(get_current_user)):
    """Получение профиля текущего пользователя"""
    return {
        "id": current_user.id,
        "username": current_user.username,
        "first_name": current_user.first_name,
        "last_name": current_user.last_name,
        "bio": current_user.bio,
        "avatar_url": current_user.avatar_url,
        "public_key": current_user.public_key,
        "is_online": current_user.is_online,
        "last_seen": current_user.last_seen,
        "created_at": current_user.created_at
    }

@app.put("/user/me")
async def update_profile(
    update_data: UserUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Обновление профиля"""
    try:
        if update_data.username:
            # Проверка уникальности username
            existing = db.query(User).filter(
                User.username == update_data.username,
                User.id != current_user.id
            ).first()
            if existing:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Username уже используется"
                )
            current_user.username = update_data.username
        
        if update_data.first_name:
            current_user.first_name = update_data.first_name
        if update_data.last_name is not None:
            current_user.last_name = update_data.last_name
        if update_data.bio is not None:
            current_user.bio = update_data.bio
        
        current_user.updated_at = datetime.utcnow()
        db.commit()
        
        logger.info(f"Профиль пользователя {current_user.username} обновлён")
        return {"message": "Профиль обновлён успешно"}
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка при обновлении профиля: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

@app.put("/user/me/password")
async def change_password(
    password_data: PasswordChange,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Смена пароля"""
    try:
        if not verify_password(password_data.old_password, current_user.password_hash):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Неверный текущий пароль"
            )
        
        current_user.password_hash = hash_password(password_data.new_password)
        current_user.updated_at = datetime.utcnow()
        db.commit()
        
        logger.info(f"Пароль пользователя {current_user.username} изменён")
        return {"message": "Пароль изменён успешно"}
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка при смене пароля: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

@app.post("/user/me/avatar")
async def upload_avatar(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Загрузка аватарки"""
    try:
        # Проверка типа файла
        if file.content_type not in ["image/jpeg", "image/png", "image/gif"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Допустимы только изображения (JPEG, PNG, GIF)"
            )
        
        # Проверка размера
        file.file.seek(0, 2)
        size = file.file.tell()
        file.file.seek(0)
        if size > MAX_FILE_SIZE:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Размер файла превышает {MAX_FILE_SIZE // 1024} КБ"
            )
        
        # Сохранение файла
        file_id = generate_file_id()
        file_extension = file.filename.split('.')[-1]
        filename = f"avatar_{current_user.id}_{file_id}.{file_extension}"
        file_path = save_upload_file(file, AVATAR_DIR, filename)
        
        # Удаление старой аватарки
        if current_user.avatar_url:
            old_path = Path(current_user.avatar_url)
            if old_path.exists():
                old_path.unlink()
        
        current_user.avatar_url = file_path
        current_user.updated_at = datetime.utcnow()
        db.commit()
        
        logger.info(f"Аватарка пользователя {current_user.username} обновлена")
        
        return {
            "message": "Аватарка загружена успешно",
            "avatar_url": file_path
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка при загрузке аватарки: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

@app.get("/user/search")
async def search_users(
    q: str = Query(..., min_length=1),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Поиск пользователей по username"""
    try:
        users = db.query(User).filter(
            User.username.contains(q),
            User.id != current_user.id
        ).limit(20).all()
        
        return [{
            "id": user.id,
            "username": user.username,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "avatar_url": user.avatar_url,
            "is_online": user.is_online
        } for user in users]
        
    except Exception as e:
        logger.error(f"Ошибка при поиске пользователей: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

# --- ЧАТЫ ---

@app.post("/chats")
async def create_chat(
    chat_data: ChatCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Создание нового чата"""
    try:
        # Создание чата
        new_chat = Chat(
            type=chat_data.type,
            name=chat_data.name if chat_data.type != "private" else None,
            description=chat_data.description if chat_data.type != "private" else None,
            created_by=current_user.id
        )
        db.add(new_chat)
        db.flush()
        
        # Добавление создателя
        creator_member = ChatMember(
            chat_id=new_chat.id,
            user_id=current_user.id,
            role="creator"
        )
        db.add(creator_member)
        
        # Добавление участников для групп и каналов
        if chat_data.type in ["group", "channel"]:
            for user_id in chat_data.members:
                if user_id != current_user.id:
                    member = ChatMember(
                        chat_id=new_chat.id,
                        user_id=user_id,
                        role="member"
                    )
                    db.add(member)
        
        db.commit()
        db.refresh(new_chat)
        
        logger.info(f"Создан чат ID: {new_chat.id} типа {chat_data.type} пользователем {current_user.username}")
        
        return {
            "id": new_chat.id,
            "type": new_chat.type,
            "name": new_chat.name,
            "created_at": new_chat.created_at
        }
        
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка при создании чата: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

@app.get("/chats")
async def get_chats(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Получение списка чатов пользователя"""
    try:
        chat_members = db.query(ChatMember).filter(
            ChatMember.user_id == current_user.id
        ).offset(skip).limit(limit).all()
        
        result = []
        for member in chat_members:
            chat = member.chat
            
            # Получение последнего сообщения
            last_message = db.query(Message).filter(
                Message.chat_id == chat.id
            ).order_by(Message.created_at.desc()).first()
            
            # Получение непрочитанных
            unread_count = db.query(Message).filter(
                Message.chat_id == chat.id,
                Message.is_read == False,
                Message.sender_id != current_user.id
            ).count()
            
            # Для приватных чатов получаем имя собеседника
            chat_name = chat.name
            if chat.type == "private":
                other_member = db.query(ChatMember).filter(
                    ChatMember.chat_id == chat.id,
                    ChatMember.user_id != current_user.id
                ).first()
                if other_member:
                    chat_name = f"{other_member.user.first_name} {other_member.user.last_name or ''}".strip()
            
            result.append({
                "id": chat.id,
                "type": chat.type,
                "name": chat_name,
                "avatar_url": chat.avatar_url,
                "last_message": {
                    "content": last_message.content[:50] if last_message else None,
                    "created_at": last_message.created_at if last_message else None,
                    "sender_id": last_message.sender_id if last_message else None
                } if last_message else None,
                "unread_count": unread_count,
                "is_pinned": member.is_pinned,
                "folder": member.folder,
                "created_at": chat.created_at
            })
        
        return result
        
    except Exception as e:
        logger.error(f"Ошибка при получении чатов: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

@app.get("/chats/{chat_id}")
async def get_chat_details(
    chat_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Детальная информация о чате"""
    try:
        # Проверка доступа
        member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if not member:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Нет доступа к этому чату"
            )
        
        chat = db.query(Chat).filter(Chat.id == chat_id).first()
        if not chat:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Чат не найден"
            )
        
        # Получение участников
        members = db.query(ChatMember).filter(ChatMember.chat_id == chat_id).all()
        members_list = [{
            "user_id": m.user_id,
            "username": m.user.username,
            "first_name": m.user.first_name,
            "last_name": m.user.last_name,
            "role": m.role,
            "joined_at": m.joined_at
        } for m in members]
        
        return {
            "id": chat.id,
            "type": chat.type,
            "name": chat.name,
            "description": chat.description,
            "avatar_url": chat.avatar_url,
            "created_by": chat.created_by,
            "created_at": chat.created_at,
            "members": members_list,
            "member_count": len(members_list)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при получении чата: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

@app.delete("/chats/{chat_id}")
async def delete_chat(
    chat_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Удаление чата (только создатель)"""
    try:
        chat = db.query(Chat).filter(Chat.id == chat_id).first()
        if not chat:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Чат не найден"
            )
        
        if chat.created_by != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Только создатель может удалить чат"
            )
        
        # Удаление всех сообщений
        db.query(Message).filter(Message.chat_id == chat_id).delete()
        
        # Удаление всех участников
        db.query(ChatMember).filter(ChatMember.chat_id == chat_id).delete()
        
        # Удаление чата
        db.delete(chat)
        db.commit()
        
        logger.info(f"Чат {chat_id} удалён пользователем {current_user.username}")
        return {"message": "Чат удалён успешно"}
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка при удалении чата: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

@app.post("/chats/{chat_id}/pin")
async def pin_chat(
    chat_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Закрепление/открепление чата"""
    try:
        member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if not member:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Нет доступа к этому чату"
            )
        
        member.is_pinned = not member.is_pinned
        db.commit()
        
        status = "закреплён" if member.is_pinned else "откреплён"
        logger.info(f"Чат {chat_id} {status} пользователем {current_user.username}")
        
        return {"message": f"Чат {status} успешно", "is_pinned": member.is_pinned}
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка при закреплении чата: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

@app.put("/chats/{chat_id}/folder")
async def move_to_folder(
    chat_id: int,
    folder: str = Query(..., max_length=50),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Перемещение чата в папку"""
    try:
        member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if not member:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Нет доступа к этому чату"
            )
        
        member.folder = folder if folder.strip() else None
        db.commit()
        
        logger.info(f"Чат {chat_id} перемещён в папку '{folder}'")
        return {"message": "Чат перемещён в папку", "folder": member.folder}
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка при перемещении чата: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

@app.post("/chats/{chat_id}/members")
async def add_member(
    chat_id: int,
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Добавление участника в чат"""
    try:
        # Проверка прав
        member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if not member or member.role not in ["creator", "admin"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Только создатель или администратор может добавлять участников"
            )
        
        # Проверка, не заблокирован ли пользователь
        if is_user_blocked(db, user_id, current_user.id):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Вы заблокировали этого пользователя"
            )
        
        # Добавление участника
        new_member = ChatMember(
            chat_id=chat_id,
            user_id=user_id,
            role="member"
        )
        db.add(new_member)
        db.commit()
        
        logger.info(f"Пользователь {user_id} добавлен в чат {chat_id}")
        return {"message": "Участник добавлен успешно"}
        
    except HTTPException:
        raise
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Пользователь уже является участником чата"
        )
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка при добавлении участника: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

@app.delete("/chats/{chat_id}/members/{user_id}")
async def remove_member(
    chat_id: int,
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Удаление участника из чата"""
    try:
        # Проверка прав
        current_member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if not current_member or current_member.role not in ["creator", "admin"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Только создатель или администратор может удалять участников"
            )
        
        # Нельзя удалить создателя
        target_member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == user_id
        ).first()
        
        if not target_member:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Участник не найден"
            )
        
        if target_member.role == "creator":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Нельзя удалить создателя чата"
            )
        
        db.delete(target_member)
        db.commit()
        
        logger.info(f"Пользователь {user_id} удалён из чата {chat_id}")
        return {"message": "Участник удалён успешно"}
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка при удалении участника: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

@app.put("/chats/{chat_id}/members/{user_id}/role")
async def change_role(
    chat_id: int,
    user_id: int,
    role: str = Query(..., regex="^(admin|member)$"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Изменение роли участника"""
    try:
        # Проверка прав
        current_member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if not current_member or current_member.role not in ["creator", "admin"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Только создатель или администратор может менять роли"
            )
        
        target_member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == user_id
        ).first()
        
        if not target_member:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Участник не найден"
            )
        
        if target_member.role == "creator":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Нельзя изменить роль создателя"
            )
        
        target_member.role = role
        db.commit()
        
        logger.info(f"Роль пользователя {user_id} в чате {chat_id} изменена на {role}")
        return {"message": f"Роль изменена на {role}"}
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка при изменении роли: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

# --- СООБЩЕНИЯ ---

@app.get("/chats/{chat_id}/messages")
async def get_messages(
    chat_id: int,
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Получение истории сообщений"""
    try:
        # Проверка доступа
        member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if not member:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Нет доступа к этому чату"
            )
        
        messages = db.query(Message).filter(
            Message.chat_id == chat_id
        ).order_by(Message.created_at.desc()).offset(skip).limit(limit).all()
        
        return [{
            "message_id": msg.message_id,
            "send_id": msg.send_id,
            "sender_id": msg.sender_id,
            "chat_id": msg.chat_id,
            "content": msg.content,
            "file_url": msg.file_url,
            "file_type": msg.file_type,
            "file_name": msg.file_name,
            "is_read": msg.is_read,
            "created_at": msg.created_at
        } for msg in messages]
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при получении сообщений: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

@app.post("/chats/{chat_id}/messages")
async def send_message(
    chat_id: int,
    message_data: MessageCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Отправка сообщения через REST (для синхронных запросов)"""
    try:
        # Проверка доступа
        member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if not member:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Нет доступа к этому чату"
            )
        
        # Проверка блокировки
        chat = db.query(Chat).filter(Chat.id == chat_id).first()
        if chat.type == "private":
            other_member = db.query(ChatMember).filter(
                ChatMember.chat_id == chat_id,
                ChatMember.user_id != current_user.id
            ).first()
            if other_member and is_user_blocked(db, current_user.id, other_member.user_id):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Вы заблокировали этого пользователя"
                )
        
        # Создание сообщения
        new_message = Message(
            message_id=message_data.message_id,
            send_id=message_data.send_id,
            chat_id=chat_id,
            sender_id=current_user.id,
            content=message_data.content,
            file_url=message_data.file_url
        )
        db.add(new_message)
        db.commit()
        db.refresh(new_message)
        
        logger.info(f"Сообщение {message_data.message_id} отправлено в чат {chat_id}")
        
        # Отправка через WebSocket
        message_response = {
            "type": "message",
            "message_id": new_message.message_id,
            "send_id": new_message.send_id,
            "sender_id": new_message.sender_id,
            "chat_id": new_message.chat_id,
            "content": new_message.content,
            "file_url": new_message.file_url,
            "created_at": new_message.created_at.isoformat()
        }
        
        # Рассылка всем участникам чата
        members = db.query(ChatMember).filter(ChatMember.chat_id == chat_id).all()
        for m in members:
            if m.user_id != current_user.id:  # Не отправляем отправителю
                await manager.send_message(m.user_id, message_response)
        
        return message_response
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка при отправке сообщения: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

@app.put("/messages/{message_id}/read")
async def mark_as_read(
    message_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Отметка сообщения как прочитанного"""
    try:
        message = db.query(Message).filter(Message.message_id == message_id).first()
        
        if not message:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Сообщение не найдено"
            )
        
        if message.sender_id == current_user.id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Нельзя отметить своё сообщение как прочитанное"
            )
        
        message.is_read = True
        message.read_at = datetime.utcnow()
        db.commit()
        
        # Отправка уведомления
        await manager.send_message(message.sender_id, {
            "type": "read",
            "message_id": message.message_id,
            "send_id": message.send_id
        })
        
        logger.info(f"Сообщение {message_id} отмечено как прочитанное пользователем {current_user.username}")
        return {"message": "Сообщение отмечено как прочитанное"}
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка при отметке прочитанного: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

@app.get("/messages/unread")
async def get_unread_count(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Количество непрочитанных сообщений"""
    try:
        # Получение всех чатов пользователя
        chat_ids = [m.chat_id for m in db.query(ChatMember).filter(
            ChatMember.user_id == current_user.id
        ).all()]
        
        unread_count = db.query(Message).filter(
            Message.chat_id.in_(chat_ids),
            Message.is_read == False,
            Message.sender_id != current_user.id
        ).count()
        
        return {"unread_count": unread_count}
        
    except Exception as e:
        logger.error(f"Ошибка при подсчёте непрочитанных: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

# --- ФАЙЛЫ ---

@app.post("/upload/temp", response_model=FileUploadResponse)
async def upload_temp_file(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Загрузка временного файла"""
    try:
        # Проверка типа файла
        allowed_types = ["image/jpeg", "image/png", "image/gif", "application/pdf", "text/plain"]
        if file.content_type not in allowed_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Неподдерживаемый тип файла"
            )
        
        # Проверка размера
        file.file.seek(0, 2)
        size = file.file.tell()
        file.file.seek(0)
        if size > MAX_FILE_SIZE:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Размер файла превышает {MAX_FILE_SIZE // 1024} КБ"
            )
        
        # Сохранение файла
        file_id = generate_file_id()
        original_filename = file.filename
        filename = f"{file_id}_{original_filename}"
        file_path = save_upload_file(file, TEMP_DIR, filename)
        
        logger.info(f"Временный файл {file_id} загружен пользователем {current_user.username}")
        
        return {
            "file_id": file_id,
            "url": file_path
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при загрузке файла: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

@app.get("/files/{file_id}")
async def get_file(
    file_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Скачивание файла"""
    try:
        # Поиск файла в папках
        temp_path = TEMP_DIR / file_id
        avatar_path = AVATAR_DIR / file_id
        
        file_path = None
        if temp_path.exists():
            file_path = temp_path
        elif avatar_path.exists():
            file_path = avatar_path
        
        if not file_path:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Файл не найден"
            )
        
        return FileResponse(
            path=str(file_path),
            filename=file_path.name
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при скачивании файла: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

# --- БЛОКИРОВКИ ---

@app.post("/blocks/{user_id}")
async def block_user(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Блокировка пользователя"""
    try:
        if user_id == current_user.id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Нельзя заблокировать самого себя"
            )
        
        # Проверка существования
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Пользователь не найден"
            )
        
        # Проверка, не заблокирован ли уже
        existing_block = db.query(UserBlock).filter(
            UserBlock.blocker_id == current_user.id,
            UserBlock.blocked_id == user_id
        ).first()
        
        if existing_block:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Пользователь уже заблокирован"
            )
        
        # Создание блокировки
        block = UserBlock(
            blocker_id=current_user.id,
            blocked_id=user_id
        )
        db.add(block)
        db.commit()
        
        logger.info(f"Пользователь {current_user.username} заблокировал {user.username}")
        return {"message": "Пользователь заблокирован"}
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка при блокировке: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

@app.delete("/blocks/{user_id}")
async def unblock_user(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Разблокировка пользователя"""
    try:
        block = db.query(UserBlock).filter(
            UserBlock.blocker_id == current_user.id,
            UserBlock.blocked_id == user_id
        ).first()
        
        if not block:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Пользователь не заблокирован"
            )
        
        db.delete(block)
        db.commit()
        
        logger.info(f"Пользователь {current_user.username} разблокировал пользователя {user_id}")
        return {"message": "Пользователь разблокирован"}
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка при разблокировке: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

@app.get("/blocks")
async def get_blocks(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Список заблокированных пользователей"""
    try:
        blocks = db.query(UserBlock).filter(
            UserBlock.blocker_id == current_user.id
        ).all()
        
        return [{
            "user_id": b.blocked_id,
            "username": b.blocked.username,
            "first_name": b.blocked.first_name,
            "last_name": b.blocked.last_name,
            "created_at": b.created_at
        } for b in blocks]
        
    except Exception as e:
        logger.error(f"Ошибка при получении списка блокировок: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

# --- КАНАЛЫ ---

@app.post("/channels/{chat_id}/subscribe")
async def subscribe_channel(
    chat_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Подписка/отписка от канала"""
    try:
        chat = db.query(Chat).filter(Chat.id == chat_id).first()
        if not chat or chat.type != "channel":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Канал не найден"
            )
        
        member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if member:
            # Отписка
            db.delete(member)
            db.commit()
            logger.info(f"Пользователь {current_user.username} отписался от канала {chat_id}")
            return {"message": "Вы отписались от канала"}
        else:
            # Подписка
            new_member = ChatMember(
                chat_id=chat_id,
                user_id=current_user.id,
                role="member"
            )
            db.add(new_member)
            db.commit()
            logger.info(f"Пользователь {current_user.username} подписался на канал {chat_id}")
            return {"message": "Вы подписались на канал"}
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка при подписке на канал: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

@app.get("/channels")
async def get_channels(
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Список публичных каналов"""
    try:
        channels = db.query(Chat).filter(
            Chat.type == "channel"
        ).offset(skip).limit(limit).all()
        
        result = []
        for channel in channels:
            # Проверка, подписан ли пользователь
            is_subscribed = db.query(ChatMember).filter(
                ChatMember.chat_id == channel.id,
                ChatMember.user_id == current_user.id
            ).first() is not None
            
            member_count = db.query(ChatMember).filter(
                ChatMember.chat_id == channel.id
            ).count()
            
            result.append({
                "id": channel.id,
                "name": channel.name,
                "description": channel.description,
                "avatar_url": channel.avatar_url,
                "is_subscribed": is_subscribed,
                "member_count": member_count,
                "created_at": channel.created_at
            })
        
        return result
        
    except Exception as e:
        logger.error(f"Ошибка при получении каналов: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Внутренняя ошибка сервера"
        )

# ======================== WEBSOCKET ========================

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket эндпоинт для реального времени"""
    user = None
    user_id = None
    
    try:
        # Получение токена из параметров запроса
        token = websocket.query_params.get("token")
        if not token:
            await websocket.close(code=1008, reason="Токен не предоставлен")
            return
        
        # Верификация токена
        try:
            payload = verify_token(token)
            user_id = int(payload.get("sub"))
        except:
            await websocket.close(code=1008, reason="Недействительный токен")
            return
        
        # Получение пользователя из БД
        db = SessionLocal()
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            await websocket.close(code=1008, reason="Пользователь не найден")
            return
        
        # Проверка блокировок
        if is_user_blocked(db, user_id, user_id):
            await websocket.close(code=1008, reason="Вы заблокированы")
            return
        
        # Подключение
        await manager.connect(websocket, user_id)
        
        # Обновление статуса
        user.is_online = True
        user.last_seen = datetime.utcnow()
        db.commit()
        
        # Уведомление о статусе
        await send_online_status(user_id, True)
        
        logger.info(f"Пользователь {user.username} подключился к WebSocket")
        
        # Обработка сообщений
        while True:
            try:
                data = await websocket.receive_json()
                
                # Обработка различных типов сообщений
                msg_type = data.get("type")
                
                if msg_type == "message":
                    # Отправка сообщения
                    chat_id = data.get("chat_id")
                    message_id = data.get("message_id")
                    send_id = data.get("send_id")
                    content = data.get("content")
                    file_url = data.get("file_url")
                    
                    if not all([chat_id, message_id, send_id, content]):
                        await websocket.send_json({
                            "type": "error",
                            "message": "Недостаточно данных для отправки сообщения"
                        })
                        continue
                    
                    # Проверка доступа
                    member = db.query(ChatMember).filter(
                        ChatMember.chat_id == chat_id,
                        ChatMember.user_id == user_id
                    ).first()
                    
                    if not member:
                        await websocket.send_json({
                            "type": "error",
                            "message": "Нет доступа к чату"
                        })
                        continue
                    
                    # Проверка блокировки
                    chat = db.query(Chat).filter(Chat.id == chat_id).first()
                    if chat.type == "private":
                        other_member = db.query(ChatMember).filter(
                            ChatMember.chat_id == chat_id,
                            ChatMember.user_id != user_id
                        ).first()
                        if other_member and is_user_blocked(db, user_id, other_member.user_id):
                            await websocket.send_json({
                                "type": "error",
                                "message": "Вы заблокировали этого пользователя"
                            })
                            continue
                    
                    # Сохранение сообщения
                    new_message = Message(
                        message_id=message_id,
                        send_id=send_id,
                        chat_id=chat_id,
                        sender_id=user_id,
                        content=content,
                        file_url=file_url
                    )
                    db.add(new_message)
                    db.commit()
                    db.refresh(new_message)
                    
                    # Подготовка ответа
                    response = {
                        "type": "message",
                        "message_id": new_message.message_id,
                        "send_id": new_message.send_id,
                        "sender_id": new_message.sender_id,
                        "chat_id": new_message.chat_id,
                        "content": new_message.content,
                        "file_url": new_message.file_url,
                        "created_at": new_message.created_at.isoformat()
                    }
                    
                    # Отправка всем участникам чата
                    members = db.query(ChatMember).filter(ChatMember.chat_id == chat_id).all()
                    for m in members:
                        # Не отправляем отправителю
                        if m.user_id != user_id:
                            await manager.send_message(m.user_id, response)
                    
                    # Подтверждение доставки отправителю
                    await websocket.send_json({
                        "type": "delivered",
                        "message_id": message_id,
                        "send_id": send_id
                    })
                    
                    logger.info(f"WebSocket сообщение {message_id} отправлено в чат {chat_id}")
                
                elif msg_type == "read":
                    # Отметка прочитанного
                    message_id = data.get("message_id")
                    send_id = data.get("send_id")
                    
                    if not message_id or not send_id:
                        await websocket.send_json({
                            "type": "error",
                            "message": "Недостаточно данных для отметки прочитанного"
                        })
                        continue
                    
                    message = db.query(Message).filter(
                        Message.message_id == message_id,
                        Message.send_id == send_id
                    ).first()
                    
                    if message and message.sender_id != user_id:
                        message.is_read = True
                        message.read_at = datetime.utcnow()
                        db.commit()
                        
                        # Отправка уведомления отправителю
                        await manager.send_message(message.sender_id, {
                            "type": "read",
                            "message_id": message_id,
                            "send_id": send_id
                        })
                        
                        logger.info(f"Сообщение {message_id} отмечено как прочитанное через WebSocket")
                
                elif msg_type == "typing":
                    # Индикатор набора текста
                    chat_id = data.get("chat_id")
                    is_typing = data.get("is_typing", False)
                    
                    if chat_id:
                        members = db.query(ChatMember).filter(ChatMember.chat_id == chat_id).all()
                        for m in members:
                            if m.user_id != user_id:
                                await manager.send_message(m.user_id, {
                                    "type": "typing",
                                    "chat_id": chat_id,
                                    "user_id": user_id,
                                    "is_typing": is_typing
                                })
            
            except WebSocketDisconnect:
                break
            except Exception as e:
                logger.error(f"Ошибка обработки WebSocket сообщения: {e}")
                await websocket.send_json({
                    "type": "error",
                    "message": "Ошибка обработки сообщения"
                })
    
    except WebSocketDisconnect:
        pass
    finally:
        # Отключение
        if user:
            user.is_online = False
            user.last_seen = datetime.utcnow()
            db.commit()
            
            # Уведомление о статусе
            await send_online_status(user_id, False)
            
            logger.info(f"Пользователь {user.username} отключился от WebSocket")
        
        if user_id:
            manager.disconnect(websocket)
        
        if 'db' in locals():
            db.close()

# ======================== ЗАПУСК ========================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
