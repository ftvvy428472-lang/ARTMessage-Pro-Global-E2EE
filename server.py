import asyncio
import base64
import hashlib
import json
import logging
import os
import shutil
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any

import bcrypt
import jwt
from fastapi import FastAPI, HTTPException, Depends, UploadFile, File, Form, Query, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field, validator
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, ForeignKey, Text, UniqueConstraint, Index, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from sqlalchemy.sql import expression
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

# ===========================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
# ===========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ===========================================
# КОНФИГУРАЦИЯ
# ===========================================
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7  # 7 дней
REFRESH_TOKEN_EXPIRE_DAYS = 30
UPLOAD_DIR = Path("uploads")
TEMP_DIR = UPLOAD_DIR / "temp"
AVATAR_DIR = UPLOAD_DIR / "avatars"
MAX_FILE_SIZE = 500 * 1024  # 500 КБ
ALLOWED_FILE_TYPES = ['image', 'document']
TEMP_FILE_LIFETIME_MINUTES = 15

# Создаем папки если их нет
UPLOAD_DIR.mkdir(exist_ok=True)
TEMP_DIR.mkdir(exist_ok=True)
AVATAR_DIR.mkdir(exist_ok=True)

# ===========================================
# БАЗА ДАННЫХ (PostgreSQL на Render)
# ===========================================
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./artmessage.db")
# Render использует postgres://, но SQLAlchemy нужен postgresql://
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
SQLALCHEMY_DATABASE_URL = DATABASE_URL

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ===========================================
# МОДЕЛИ БД (SQLAlchemy)
# ===========================================
class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, index=True, nullable=False)
    first_name = Column(String(50), nullable=False)
    last_name = Column(String(50), nullable=True)
    bio = Column(String(500), nullable=True)
    password_hash = Column(String(128), nullable=False)
    avatar_url = Column(String(500), nullable=True)
    public_key = Column(String(5000), nullable=False)
    hardware_id = Column(String(255), unique=True, nullable=False)
    is_online = Column(Boolean, default=False)
    last_seen = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    messages = relationship("Message", foreign_keys="Message.sender_id", back_populates="sender")
    chats_created = relationship("Chat", foreign_keys="Chat.created_by", back_populates="creator")
    memberships = relationship("ChatMember", back_populates="user")
    blocks_given = relationship("UserBlock", foreign_keys="UserBlock.blocker_id", back_populates="blocker")
    blocks_received = relationship("UserBlock", foreign_keys="UserBlock.blocked_id", back_populates="blocked")

class Chat(Base):
    __tablename__ = "chats"
    
    id = Column(Integer, primary_key=True, index=True)
    type = Column(String(20), nullable=False)
    name = Column(String(100), nullable=True)
    description = Column(String(500), nullable=True)
    avatar_url = Column(String(500), nullable=True)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    creator = relationship("User", foreign_keys=[created_by], back_populates="chats_created")
    members = relationship("ChatMember", back_populates="chat")
    messages = relationship("Message", back_populates="chat")

class ChatMember(Base):
    __tablename__ = "chat_members"
    
    id = Column(Integer, primary_key=True, index=True)
    chat_id = Column(Integer, ForeignKey("chats.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    role = Column(String(20), default='member')
    is_pinned = Column(Boolean, default=False)
    folder = Column(String(50), nullable=True)
    joined_at = Column(DateTime, default=datetime.utcnow)
    
    chat = relationship("Chat", back_populates="members")
    user = relationship("User", back_populates="memberships")
    
    __table_args__ = (
        UniqueConstraint('chat_id', 'user_id', name='unique_chat_member'),
    )

class Message(Base):
    __tablename__ = "messages"
    
    id = Column(Integer, primary_key=True, index=True)
    message_id = Column(String(36), unique=True, index=True, nullable=False)
    send_id = Column(String(36), nullable=False)
    chat_id = Column(Integer, ForeignKey("chats.id"), nullable=False)
    sender_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    content = Column(Text, nullable=False)
    file_url = Column(String(500), nullable=True)
    file_type = Column(String(20), nullable=True)
    file_name = Column(String(255), nullable=True)
    is_read = Column(Boolean, default=False)
    read_at = Column(DateTime, nullable=True)
    delivered_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    chat = relationship("Chat", back_populates="messages")
    sender = relationship("User", foreign_keys=[sender_id], back_populates="messages")

class UserBlock(Base):
    __tablename__ = "user_blocks"
    
    id = Column(Integer, primary_key=True, index=True)
    blocker_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    blocked_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    blocker = relationship("User", foreign_keys=[blocker_id], back_populates="blocks_given")
    blocked = relationship("User", foreign_keys=[blocked_id], back_populates="blocks_received")
    
    __table_args__ = (
        UniqueConstraint('blocker_id', 'blocked_id', name='unique_block'),
    )

# ===========================================
# PYDANTIC МОДЕЛИ
# ===========================================
class UserRegister(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    first_name: str = Field(..., min_length=1, max_length=50)
    last_name: Optional[str] = Field(None, max_length=50)
    bio: Optional[str] = Field(None, max_length=500)
    password: str = Field(..., min_length=6)
    public_key: str = Field(..., min_length=10)
    hardware_id: str = Field(..., min_length=10)
    
    @validator('username')
    def validate_username(cls, v):
        if not v.isalnum():
            raise ValueError('Username must contain only letters and numbers')
        return v

class UserLogin(BaseModel):
    username: str
    password: str
    hardware_id: str

class UserUpdate(BaseModel):
    first_name: Optional[str] = Field(None, min_length=1, max_length=50)
    last_name: Optional[str] = Field(None, max_length=50)
    username: Optional[str] = Field(None, min_length=3, max_length=50)
    bio: Optional[str] = Field(None, max_length=500)

class PasswordChange(BaseModel):
    old_password: str
    new_password: str = Field(..., min_length=6)

class ChatCreate(BaseModel):
    type: str = Field(..., regex='^(private|group|channel)$')
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = Field(None, max_length=500)
    user_ids: Optional[List[int]] = []

class MessageCreate(BaseModel):
    content: str = Field(..., min_length=1)
    message_id: str = Field(..., regex='^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')
    send_id: str = Field(..., regex='^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')

class MemberAdd(BaseModel):
    user_id: int

class MemberRoleUpdate(BaseModel):
    role: str = Field(..., regex='^(admin|member)$')

class Token(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"

# ===========================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ===========================================
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def hash_password(password: str) -> str:
    salt = bcrypt.gensalt(rounds=12)
    return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')

def verify_password(password: str, password_hash: str) -> bool:
    return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))

def create_jwt_token(data: dict, expires_delta: timedelta) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + expires_delta
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def decode_jwt_token(token: str) -> dict:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.PyJWTError:
        return {}

def get_current_user(token: str, db: Session) -> User:
    payload = decode_jwt_token(token)
    user_id = payload.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token")
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    
    return user

def generate_filename(original_filename: str) -> str:
    ext = Path(original_filename).suffix
    return f"{uuid.uuid4().hex}{ext}"

def save_file(file: UploadFile, directory: Path, max_size: int = MAX_FILE_SIZE) -> str:
    file.file.seek(0, 2)
    size = file.file.tell()
    file.file.seek(0)
    
    if size > max_size:
        raise HTTPException(400, f"File too large. Max size: {max_size} bytes")
    
    filename = generate_filename(file.filename)
    filepath = directory / filename
    
    with open(filepath, "wb") as f:
        shutil.copyfileobj(file.file, f)
    
    return str(filepath.relative_to("."))

def delete_old_temp_files():
    try:
        now = datetime.utcnow()
        for filepath in TEMP_DIR.iterdir():
            if filepath.is_file():
                mtime = datetime.fromtimestamp(filepath.stat().st_mtime)
                if (now - mtime) > timedelta(minutes=TEMP_FILE_LIFETIME_MINUTES):
                    filepath.unlink()
                    logger.info(f"Deleted old temp file: {filepath}")
    except Exception as e:
        logger.error(f"Error deleting temp files: {e}")

# ===========================================
# СОЗДАНИЕ ТАБЛИЦ
# ===========================================
Base.metadata.create_all(bind=engine)
logger.info("Database tables created successfully")

# ===========================================
# FASTAPI ПРИЛОЖЕНИЕ
# ===========================================
app = FastAPI(title="ARTMessage API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===========================================
# ШЕДУЛЕР
# ===========================================
scheduler = BackgroundScheduler()
scheduler.add_job(
    delete_old_temp_files,
    trigger=IntervalTrigger(minutes=5),
    id='delete_temp_files',
    replace_existing=True
)
scheduler.start()
logger.info("Background scheduler started")

# ===========================================
# АУТЕНТИФИКАЦИЯ
# ===========================================
@app.post("/auth/register")
async def register(user_data: UserRegister, db: Session = Depends(get_db)):
    try:
        if db.query(User).filter(User.username == user_data.username).first():
            raise HTTPException(400, "Username already taken")
        
        if db.query(User).filter(User.hardware_id == user_data.hardware_id).first():
            raise HTTPException(400, "Hardware ID already registered")
        
        hashed_password = hash_password(user_data.password)
        new_user = User(
            username=user_data.username,
            first_name=user_data.first_name,
            last_name=user_data.last_name,
            bio=user_data.bio,
            password_hash=hashed_password,
            public_key=user_data.public_key,
            hardware_id=user_data.hardware_id,
            created_at=datetime.utcnow()
        )
        
        db.add(new_user)
        db.commit()
        db.refresh(new_user)
        
        logger.info(f"User registered: {new_user.username} (ID: {new_user.id})")
        
        return {
            "message": "User registered successfully",
            "user_id": new_user.id,
            "username": new_user.username
        }
    
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Registration error: {e}")
        raise HTTPException(500, f"Registration failed: {str(e)}")

@app.post("/auth/login", response_model=Token)
async def login(user_data: UserLogin, db: Session = Depends(get_db)):
    try:
        user = db.query(User).filter(User.username == user_data.username).first()
        if not user:
            raise HTTPException(401, "Invalid username or password")
        
        if not verify_password(user_data.password, user.password_hash):
            raise HTTPException(401, "Invalid username or password")
        
        if user.hardware_id != user_data.hardware_id:
            raise HTTPException(401, "Invalid hardware ID")
        
        access_token = create_jwt_token(
            {"user_id": user.id},
            timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        )
        refresh_token = create_jwt_token(
            {"user_id": user.id},
            timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
        )
        
        logger.info(f"User logged in: {user.username} (ID: {user.id})")
        
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Login error: {e}")
        raise HTTPException(500, f"Login failed: {str(e)}")

@app.post("/auth/refresh", response_model=Token)
async def refresh_token(refresh_token: str = Form(...), db: Session = Depends(get_db)):
    try:
        payload = decode_jwt_token(refresh_token)
        user_id = payload.get("user_id")
        if not user_id:
            raise HTTPException(401, "Invalid refresh token")
        
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(401, "User not found")
        
        new_access_token = create_jwt_token(
            {"user_id": user.id},
            timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        )
        new_refresh_token = create_jwt_token(
            {"user_id": user.id},
            timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
        )
        
        return {
            "access_token": new_access_token,
            "refresh_token": new_refresh_token,
            "token_type": "bearer"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Token refresh error: {e}")
        raise HTTPException(500, f"Token refresh failed: {str(e)}")

@app.post("/auth/logout")
async def logout(token: str = Form(...), db: Session = Depends(get_db)):
    try:
        user = get_current_user(token, db)
        logger.info(f"User logged out: {user.username} (ID: {user.id})")
        return {"message": "Logged out successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Logout error: {e}")
        raise HTTPException(500, f"Logout failed: {str(e)}")

# ===========================================
# ПРОФИЛЬ
# ===========================================
@app.get("/user/me")
async def get_my_profile(token: str = Form(...), db: Session = Depends(get_db)):
    try:
        user = get_current_user(token, db)
        return {
            "id": user.id,
            "username": user.username,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "bio": user.bio,
            "avatar_url": user.avatar_url,
            "public_key": user.public_key,
            "hardware_id": user.hardware_id,
            "is_online": user.is_online,
            "last_seen": user.last_seen.isoformat() if user.last_seen else None,
            "created_at": user.created_at.isoformat() if user.created_at else None
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get profile error: {e}")
        raise HTTPException(500, f"Failed to get profile: {str(e)}")

@app.put("/user/me")
async def update_my_profile(
    token: str = Form(...),
    update_data: UserUpdate = Depends(),
    db: Session = Depends(get_db)
):
    try:
        user = get_current_user(token, db)
        
        if update_data.username and update_data.username != user.username:
            existing = db.query(User).filter(User.username == update_data.username).first()
            if existing:
                raise HTTPException(400, "Username already taken")
            user.username = update_data.username
        
        if update_data.first_name:
            user.first_name = update_data.first_name
        if update_data.last_name is not None:
            user.last_name = update_data.last_name
        if update_data.bio is not None:
            user.bio = update_data.bio
        
        user.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(user)
        
        logger.info(f"Profile updated for user: {user.username}")
        return {"message": "Profile updated successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Update profile error: {e}")
        raise HTTPException(500, f"Failed to update profile: {str(e)}")

@app.put("/user/me/password")
async def change_password(
    token: str = Form(...),
    password_data: PasswordChange = Depends(),
    db: Session = Depends(get_db)
):
    try:
        user = get_current_user(token, db)
        
        if not verify_password(password_data.old_password, user.password_hash):
            raise HTTPException(400, "Current password is incorrect")
        
        user.password_hash = hash_password(password_data.new_password)
        user.updated_at = datetime.utcnow()
        db.commit()
        
        logger.info(f"Password changed for user: {user.username}")
        return {"message": "Password changed successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Change password error: {e}")
        raise HTTPException(500, f"Failed to change password: {str(e)}")

@app.post("/user/me/avatar")
async def upload_avatar(
    token: str = Form(...),
    avatar: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    try:
        user = get_current_user(token, db)
        
        if not avatar.content_type.startswith('image/'):
            raise HTTPException(400, "Only images are allowed for avatar")
        
        filename = generate_filename(avatar.filename)
        filepath = AVATAR_DIR / filename
        with open(filepath, "wb") as f:
            shutil.copyfileobj(avatar.file, f)
        
        if user.avatar_url:
            old_path = Path(user.avatar_url)
            if old_path.exists() and old_path.parent == AVATAR_DIR:
                old_path.unlink()
        
        user.avatar_url = str(filepath)
        user.updated_at = datetime.utcnow()
        db.commit()
        
        logger.info(f"Avatar uploaded for user: {user.username}")
        return {"message": "Avatar uploaded successfully", "avatar_url": str(filepath)}
    
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Upload avatar error: {e}")
        raise HTTPException(500, f"Failed to upload avatar: {str(e)}")

@app.get("/user/search")
async def search_users(
    q: str = Query(..., min_length=1),
    token: str = Form(...),
    db: Session = Depends(get_db)
):
    try:
        current_user = get_current_user(token, db)
        
        users = db.query(User).filter(
            User.username.like(f"{q}%"),
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
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Search users error: {e}")
        raise HTTPException(500, f"Search failed: {str(e)}")

# ===========================================
# ЧАТЫ
# ===========================================
@app.post("/chats")
async def create_chat(
    token: str = Form(...),
    chat_data: ChatCreate = Depends(),
    db: Session = Depends(get_db)
):
    try:
        current_user = get_current_user(token, db)
        
        if chat_data.type == 'private' and len(chat_data.user_ids) != 1:
            raise HTTPException(400, "Private chat must have exactly one other user")
        
        new_chat = Chat(
            type=chat_data.type,
            name=chat_data.name,
            description=chat_data.description,
            created_by=current_user.id,
            created_at=datetime.utcnow()
        )
        db.add(new_chat)
        db.flush()
        
        creator_member = ChatMember(
            chat_id=new_chat.id,
            user_id=current_user.id,
            role='creator',
            joined_at=datetime.utcnow()
        )
        db.add(creator_member)
        
        for user_id in chat_data.user_ids:
            user = db.query(User).filter(User.id == user_id).first()
            if not user:
                raise HTTPException(400, f"User with ID {user_id} not found")
            
            if user_id == current_user.id:
                continue
            
            member = ChatMember(
                chat_id=new_chat.id,
                user_id=user_id,
                role='member' if chat_data.type != 'channel' else 'member',
                joined_at=datetime.utcnow()
            )
            db.add(member)
        
        db.commit()
        db.refresh(new_chat)
        
        logger.info(f"Chat created: {new_chat.id} ({new_chat.type}) by user {current_user.username}")
        
        return {
            "id": new_chat.id,
            "type": new_chat.type,
            "name": new_chat.name,
            "created_at": new_chat.created_at.isoformat()
        }
    
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Create chat error: {e}")
        raise HTTPException(500, f"Failed to create chat: {str(e)}")

@app.get("/chats")
async def get_chats(
    token: str = Form(...),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    try:
        current_user = get_current_user(token, db)
        
        member_chats = db.query(ChatMember).filter(
            ChatMember.user_id == current_user.id
        ).order_by(ChatMember.is_pinned.desc(), ChatMember.joined_at.desc()).offset(offset).limit(limit).all()
        
        result = []
        for member in member_chats:
            chat = db.query(Chat).filter(Chat.id == member.chat_id).first()
            if not chat:
                continue
            
            last_message = db.query(Message).filter(
                Message.chat_id == chat.id
            ).order_by(Message.created_at.desc()).first()
            
            result.append({
                "id": chat.id,
                "type": chat.type,
                "name": chat.name or "Chat",
                "avatar_url": chat.avatar_url,
                "is_pinned": member.is_pinned,
                "folder": member.folder,
                "last_message": {
                    "content": last_message.content[:50] + "..." if last_message and len(last_message.content) > 50 else last_message.content if last_message else None,
                    "sender_id": last_message.sender_id if last_message else None,
                    "created_at": last_message.created_at.isoformat() if last_message else None
                } if last_message else None,
                "unread_count": db.query(Message).filter(
                    Message.chat_id == chat.id,
                    Message.is_read == False,
                    Message.sender_id != current_user.id
                ).count()
            })
        
        return {
            "chats": result,
            "total": db.query(ChatMember).filter(ChatMember.user_id == current_user.id).count()
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get chats error: {e}")
        raise HTTPException(500, f"Failed to get chats: {str(e)}")

@app.get("/chats/{chat_id}")
async def get_chat_details(
    chat_id: int,
    token: str = Form(...),
    db: Session = Depends(get_db)
):
    try:
        current_user = get_current_user(token, db)
        
        member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if not member:
            raise HTTPException(403, "You are not a member of this chat")
        
        chat = db.query(Chat).filter(Chat.id == chat_id).first()
        if not chat:
            raise HTTPException(404, "Chat not found")
        
        members = db.query(ChatMember, User).join(
            User, ChatMember.user_id == User.id
        ).filter(ChatMember.chat_id == chat_id).all()
        
        return {
            "id": chat.id,
            "type": chat.type,
            "name": chat.name,
            "description": chat.description,
            "avatar_url": chat.avatar_url,
            "created_by": chat.created_by,
            "created_at": chat.created_at.isoformat(),
            "members": [{
                "user_id": m.User.id,
                "username": m.User.username,
                "first_name": m.User.first_name,
                "last_name": m.User.last_name,
                "avatar_url": m.User.avatar_url,
                "role": m.ChatMember.role,
                "joined_at": m.ChatMember.joined_at.isoformat()
            } for m in members],
            "member_count": len(members)
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get chat details error: {e}")
        raise HTTPException(500, f"Failed to get chat details: {str(e)}")

@app.delete("/chats/{chat_id}")
async def delete_chat(
    chat_id: int,
    token: str = Form(...),
    db: Session = Depends(get_db)
):
    try:
        current_user = get_current_user(token, db)
        
        chat = db.query(Chat).filter(Chat.id == chat_id).first()
        if not chat:
            raise HTTPException(404, "Chat not found")
        
        member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if not member or (member.role not in ['creator', 'admin']):
            raise HTTPException(403, "Only creator or admin can delete this chat")
        
        db.query(Message).filter(Message.chat_id == chat_id).delete()
        db.query(ChatMember).filter(ChatMember.chat_id == chat_id).delete()
        db.delete(chat)
        db.commit()
        
        logger.info(f"Chat {chat_id} deleted by user {current_user.username}")
        return {"message": "Chat deleted successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Delete chat error: {e}")
        raise HTTPException(500, f"Failed to delete chat: {str(e)}")

@app.post("/chats/{chat_id}/pin")
async def toggle_pin_chat(
    chat_id: int,
    token: str = Form(...),
    db: Session = Depends(get_db)
):
    try:
        current_user = get_current_user(token, db)
        
        member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if not member:
            raise HTTPException(403, "You are not a member of this chat")
        
        member.is_pinned = not member.is_pinned
        db.commit()
        
        logger.info(f"Chat {chat_id} pin toggled to {member.is_pinned} by user {current_user.username}")
        return {"message": f"Chat {'pinned' if member.is_pinned else 'unpinned'} successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Toggle pin chat error: {e}")
        raise HTTPException(500, f"Failed to toggle pin: {str(e)}")

@app.put("/chats/{chat_id}/folder")
async def move_chat_to_folder(
    chat_id: int,
    folder_name: str = Form(...),
    token: str = Form(...),
    db: Session = Depends(get_db)
):
    try:
        current_user = get_current_user(token, db)
        
        member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if not member:
            raise HTTPException(403, "You are not a member of this chat")
        
        member.folder = folder_name if folder_name.strip() else None
        db.commit()
        
        logger.info(f"Chat {chat_id} moved to folder '{folder_name}' by user {current_user.username}")
        return {"message": f"Chat moved to folder '{folder_name}' successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Move chat to folder error: {e}")
        raise HTTPException(500, f"Failed to move chat to folder: {str(e)}")

@app.post("/chats/{chat_id}/members")
async def add_chat_member(
    chat_id: int,
    token: str = Form(...),
    member_data: MemberAdd = Depends(),
    db: Session = Depends(get_db)
):
    try:
        current_user = get_current_user(token, db)
        
        chat = db.query(Chat).filter(Chat.id == chat_id).first()
        if not chat:
            raise HTTPException(404, "Chat not found")
        
        admin_member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id,
            ChatMember.role.in_(['creator', 'admin'])
        ).first()
        
        if not admin_member:
            raise HTTPException(403, "Only admin or creator can add members")
        
        blocked = db.query(UserBlock).filter(
            (UserBlock.blocker_id == member_data.user_id) & (UserBlock.blocked_id == current_user.id) |
            (UserBlock.blocker_id == current_user.id) & (UserBlock.blocked_id == member_data.user_id)
        ).first()
        
        if blocked:
            raise HTTPException(403, "Cannot add blocked user")
        
        existing = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == member_data.user_id
        ).first()
        
        if existing:
            raise HTTPException(400, "User is already a member of this chat")
        
        user = db.query(User).filter(User.id == member_data.user_id).first()
        if not user:
            raise HTTPException(404, "User not found")
        
        new_member = ChatMember(
            chat_id=chat_id,
            user_id=member_data.user_id,
            role='member',
            joined_at=datetime.utcnow()
        )
        db.add(new_member)
        db.commit()
        
        logger.info(f"User {member_data.user_id} added to chat {chat_id} by {current_user.username}")
        return {"message": "User added to chat successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Add chat member error: {e}")
        raise HTTPException(500, f"Failed to add member: {str(e)}")

@app.delete("/chats/{chat_id}/members/{user_id}")
async def remove_chat_member(
    chat_id: int,
    user_id: int,
    token: str = Form(...),
    db: Session = Depends(get_db)
):
    try:
        current_user = get_current_user(token, db)
        
        chat = db.query(Chat).filter(Chat.id == chat_id).first()
        if not chat:
            raise HTTPException(404, "Chat not found")
        
        admin_member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id,
            ChatMember.role.in_(['creator', 'admin'])
        ).first()
        
        if not admin_member:
            raise HTTPException(403, "Only admin or creator can remove members")
        
        target_member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == user_id
        ).first()
        
        if not target_member:
            raise HTTPException(404, "User is not a member of this chat")
        
        if target_member.role == 'creator':
            raise HTTPException(403, "Cannot remove the creator of the chat")
        
        db.delete(target_member)
        db.commit()
        
        logger.info(f"User {user_id} removed from chat {chat_id} by {current_user.username}")
        return {"message": "User removed from chat successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Remove chat member error: {e}")
        raise HTTPException(500, f"Failed to remove member: {str(e)}")

@app.put("/chats/{chat_id}/members/{user_id}/role")
async def update_member_role(
    chat_id: int,
    user_id: int,
    token: str = Form(...),
    role_data: MemberRoleUpdate = Depends(),
    db: Session = Depends(get_db)
):
    try:
        current_user = get_current_user(token, db)
        
        creator_member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id,
            ChatMember.role == 'creator'
        ).first()
        
        if not creator_member:
            raise HTTPException(403, "Only creator can change roles")
        
        target_member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == user_id
        ).first()
        
        if not target_member:
            raise HTTPException(404, "User is not a member of this chat")
        
        if target_member.role == 'creator':
            raise HTTPException(403, "Cannot change role of creator")
        
        target_member.role = role_data.role
        db.commit()
        
        logger.info(f"Role of user {user_id} in chat {chat_id} changed to {role_data.role} by {current_user.username}")
        return {"message": f"Role updated to {role_data.role} successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Update member role error: {e}")
        raise HTTPException(500, f"Failed to update role: {str(e)}")

# ===========================================
# СООБЩЕНИЯ
# ===========================================
@app.get("/chats/{chat_id}/messages")
async def get_chat_messages(
    chat_id: int,
    token: str = Form(...),
    limit: int = Query(20, ge=1, le=100),
    before: Optional[str] = None,
    db: Session = Depends(get_db)
):
    try:
        current_user = get_current_user(token, db)
        
        member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if not member:
            raise HTTPException(403, "You are not a member of this chat")
        
        blocked = db.query(UserBlock).filter(
            (UserBlock.blocker_id == current_user.id) | (UserBlock.blocked_id == current_user.id)
        ).all()
        blocked_ids = [b.blocked_id if b.blocker_id == current_user.id else b.blocker_id for b in blocked]
        
        query = db.query(Message).filter(
            Message.chat_id == chat_id,
            Message.sender_id.notin_(blocked_ids)
        ).order_by(Message.created_at.desc())
        
        if before:
            before_date = datetime.fromisoformat(before.replace('Z', '+00:00'))
            query = query.filter(Message.created_at < before_date)
        
        messages = query.limit(limit).all()
        
        return [{
            "id": m.id,
            "message_id": m.message_id,
            "send_id": m.send_id,
            "sender_id": m.sender_id,
            "content": m.content,
            "file_url": m.file_url,
            "file_type": m.file_type,
            "file_name": m.file_name,
            "is_read": m.is_read,
            "created_at": m.created_at.isoformat()
        } for m in messages]
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get chat messages error: {e}")
        raise HTTPException(500, f"Failed to get messages: {str(e)}")

@app.post("/chats/{chat_id}/messages")
async def send_message(
    chat_id: int,
    token: str = Form(...),
    message_data: MessageCreate = Depends(),
    db: Session = Depends(get_db)
):
    try:
        current_user = get_current_user(token, db)
        
        member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if not member:
            raise HTTPException(403, "You are not a member of this chat")
        
        blocked = db.query(UserBlock).filter(
            (UserBlock.blocker_id == current_user.id) | (UserBlock.blocked_id == current_user.id)
        ).first()
        
        if blocked:
            raise HTTPException(403, "You are blocked or you blocked this user")
        
        existing = db.query(Message).filter(
            Message.message_id == message_data.message_id
        ).first()
        
        if existing:
            raise HTTPException(400, "Message ID already exists")
        
        new_message = Message(
            message_id=message_data.message_id,
            send_id=message_data.send_id,
            chat_id=chat_id,
            sender_id=current_user.id,
            content=message_data.content,
            created_at=datetime.utcnow()
        )
        
        db.add(new_message)
        db.commit()
        db.refresh(new_message)
        
        logger.info(f"Message {new_message.message_id} sent in chat {chat_id} by {current_user.username}")
        
        return {
            "message_id": new_message.message_id,
            "send_id": new_message.send_id,
            "created_at": new_message.created_at.isoformat()
        }
    
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Send message error: {e}")
        raise HTTPException(500, f"Failed to send message: {str(e)}")

@app.put("/messages/{message_id}/read")
async def mark_message_read(
    message_id: str,
    token: str = Form(...),
    db: Session = Depends(get_db)
):
    try:
        current_user = get_current_user(token, db)
        
        message = db.query(Message).filter(Message.message_id == message_id).first()
        if not message:
            raise HTTPException(404, "Message not found")
        
        chat = db.query(Chat).filter(Chat.id == message.chat_id).first()
        if not chat:
            raise HTTPException(404, "Chat not found")
        
        member = db.query(ChatMember).filter(
            ChatMember.chat_id == message.chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if not member:
            raise HTTPException(403, "You are not a member of this chat")
        
        if message.sender_id == current_user.id:
            raise HTTPException(400, "Cannot mark your own message as read")
        
        message.is_read = True
        message.read_at = datetime.utcnow()
        db.commit()
        
        logger.info(f"Message {message_id} marked as read by {current_user.username}")
        return {"message": "Message marked as read"}
    
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Mark message read error: {e}")
        raise HTTPException(500, f"Failed to mark message as read: {str(e)}")

@app.get("/messages/unread")
async def get_unread_count(
    token: str = Form(...),
    db: Session = Depends(get_db)
):
    try:
        current_user = get_current_user(token, db)
        
        member_chats = db.query(ChatMember).filter(
            ChatMember.user_id == current_user.id
        ).all()
        
        chat_ids = [m.chat_id for m in member_chats]
        
        unread_count = db.query(Message).filter(
            Message.chat_id.in_(chat_ids),
            Message.is_read == False,
            Message.sender_id != current_user.id
        ).count()
        
        return {"unread_count": unread_count}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get unread count error: {e}")
        raise HTTPException(500, f"Failed to get unread count: {str(e)}")

# ===========================================
# ФАЙЛЫ
# ===========================================
@app.post("/upload/temp")
async def upload_temp_file(
    token: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    try:
        current_user = get_current_user(token, db)
        
        file_type = 'image' if file.content_type.startswith('image/') else 'document'
        if file_type not in ALLOWED_FILE_TYPES:
            raise HTTPException(400, "Only images and documents are allowed")
        
        filepath = save_file(file, TEMP_DIR)
        
        logger.info(f"Temporary file uploaded: {filepath} by {current_user.username}")
        
        return {
            "file_url": filepath,
            "file_type": file_type,
            "file_name": file.filename,
            "message": "File uploaded successfully (temp file will be deleted after 15 minutes)"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload temp file error: {e}")
        raise HTTPException(500, f"Failed to upload file: {str(e)}")

@app.get("/files/{file_id}")
async def download_file(
    file_id: str,
    token: str = Form(...),
    db: Session = Depends(get_db)
):
    try:
        current_user = get_current_user(token, db)
        
        file_path = Path("uploads") / file_id
        if not file_path.exists():
            raise HTTPException(404, "File not found")
        
        message = db.query(Message).filter(
            Message.file_url == str(file_path)
        ).first()
        
        if message:
            member = db.query(ChatMember).filter(
                ChatMember.chat_id == message.chat_id,
                ChatMember.user_id == current_user.id
            ).first()
            
            if not member:
                raise HTTPException(403, "You don't have access to this file")
        
        return FileResponse(file_path)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Download file error: {e}")
        raise HTTPException(500, f"Failed to download file: {str(e)}")

# ===========================================
# БЛОКИРОВКИ
# ===========================================
@app.post("/blocks/{user_id}")
async def block_user(
    user_id: int,
    token: str = Form(...),
    db: Session = Depends(get_db)
):
    try:
        current_user = get_current_user(token, db)
        
        if user_id == current_user.id:
            raise HTTPException(400, "Cannot block yourself")
        
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(404, "User not found")
        
        existing = db.query(UserBlock).filter(
            UserBlock.blocker_id == current_user.id,
            UserBlock.blocked_id == user_id
        ).first()
        
        if existing:
            raise HTTPException(400, "User is already blocked")
        
        block = UserBlock(
            blocker_id=current_user.id,
            blocked_id=user_id,
            created_at=datetime.utcnow()
        )
        db.add(block)
        db.commit()
        
        logger.info(f"User {current_user.username} blocked user {user.username}")
        return {"message": "User blocked successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Block user error: {e}")
        raise HTTPException(500, f"Failed to block user: {str(e)}")

@app.delete("/blocks/{user_id}")
async def unblock_user(
    user_id: int,
    token: str = Form(...),
    db: Session = Depends(get_db)
):
    try:
        current_user = get_current_user(token, db)
        
        block = db.query(UserBlock).filter(
            UserBlock.blocker_id == current_user.id,
            UserBlock.blocked_id == user_id
        ).first()
        
        if not block:
            raise HTTPException(404, "User is not blocked")
        
        db.delete(block)
        db.commit()
        
        logger.info(f"User {current_user.username} unblocked user {user_id}")
        return {"message": "User unblocked successfully"}
    
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Unblock user error: {e}")
        raise HTTPException(500, f"Failed to unblock user: {str(e)}")

@app.get("/blocks")
async def get_blocked_users(
    token: str = Form(...),
    db: Session = Depends(get_db)
):
    try:
        current_user = get_current_user(token, db)
        
        blocks = db.query(UserBlock, User).join(
            User, UserBlock.blocked_id == User.id
        ).filter(UserBlock.blocker_id == current_user.id).all()
        
        return [{
            "user_id": b.User.id,
            "username": b.User.username,
            "first_name": b.User.first_name,
            "last_name": b.User.last_name,
            "avatar_url": b.User.avatar_url,
            "blocked_at": b.UserBlock.created_at.isoformat()
        } for b in blocks]
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get blocked users error: {e}")
        raise HTTPException(500, f"Failed to get blocked users: {str(e)}")

# ===========================================
# КАНАЛЫ
# ===========================================
@app.get("/channels")
async def get_public_channels(
    token: str = Form(...),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    try:
        current_user = get_current_user(token, db)
        
        channels = db.query(Chat).filter(
            Chat.type == 'channel'
        ).offset(offset).limit(limit).all()
        
        result = []
        for channel in channels:
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
                "created_by": channel.created_by,
                "created_at": channel.created_at.isoformat(),
                "member_count": member_count,
                "is_subscribed": is_subscribed
            })
        
        return {"channels": result}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get channels error: {e}")
        raise HTTPException(500, f"Failed to get channels: {str(e)}")

@app.post("/channels/{chat_id}/subscribe")
async def toggle_channel_subscription(
    chat_id: int,
    token: str = Form(...),
    db: Session = Depends(get_db)
):
    try:
        current_user = get_current_user(token, db)
        
        chat = db.query(Chat).filter(
            Chat.id == chat_id,
            Chat.type == 'channel'
        ).first()
        
        if not chat:
            raise HTTPException(404, "Channel not found")
        
        member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id == current_user.id
        ).first()
        
        if member:
            db.delete(member)
            db.commit()
            logger.info(f"User {current_user.username} unsubscribed from channel {chat_id}")
            return {"message": "Unsubscribed from channel"}
        else:
            new_member = ChatMember(
                chat_id=chat_id,
                user_id=current_user.id,
                role='member',
                joined_at=datetime.utcnow()
            )
            db.add(new_member)
            db.commit()
            logger.info(f"User {current_user.username} subscribed to channel {chat_id}")
            return {"message": "Subscribed to channel"}
    
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Toggle channel subscription error: {e}")
        raise HTTPException(500, f"Failed to toggle subscription: {str(e)}")

# ===========================================
# WEBSOCKET
# ===========================================
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[int, WebSocket] = {}

    async def connect(self, websocket: WebSocket, user_id: int):
        await websocket.accept()
        self.active_connections[user_id] = websocket
        logger.info(f"User {user_id} connected via WebSocket")

    def disconnect(self, user_id: int):
        if user_id in self.active_connections:
            del self.active_connections[user_id]
        logger.info(f"User {user_id} disconnected from WebSocket")

    async def send_personal_message(self, message: dict, user_id: int):
        if user_id in self.active_connections:
            try:
                await self.active_connections[user_id].send_json(message)
                return True
            except Exception as e:
                logger.error(f"Error sending message to user {user_id}: {e}")
                return False
        return False

    async def broadcast_to_chat(self, message: dict, chat_id: int, db: Session):
        members = db.query(ChatMember).filter(ChatMember.chat_id == chat_id).all()
        for member in members:
            await self.send_personal_message(message, member.user_id)

manager = ConnectionManager()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, token: str = Query(...), db: Session = Depends(get_db)):
    user = None
    try:
        user = get_current_user(token, db)
        await manager.connect(websocket, user.id)
        
        user.is_online = True
        user.last_seen = datetime.utcnow()
        db.commit()
        
        await manager.broadcast_to_chat({
            "type": "status",
            "user_id": user.id,
            "is_online": True
        }, None, db)
        
        logger.info(f"WebSocket connected for user {user.username} (ID: {user.id})")
        
        while True:
            try:
                data = await websocket.receive_text()
                message = json.loads(data)
                message_type = message.get("type")
                
                if message_type == "message":
                    chat_id = message.get("chat_id")
                    message_id = message.get("message_id")
                    send_id = message.get("send_id")
                    content = message.get("content")
                    file_url = message.get("file_url")
                    file_type = message.get("file_type")
                    file_name = message.get("file_name")
                    
                    member = db.query(ChatMember).filter(
                        ChatMember.chat_id == chat_id,
                        ChatMember.user_id == user.id
                    ).first()
                    
                    if not member:
                        await websocket.send_json({
                            "type": "error",
                            "message": "You are not a member of this chat"
                        })
                        continue
                    
                    blocked = db.query(UserBlock).filter(
                        (UserBlock.blocker_id == user.id) | (UserBlock.blocked_id == user.id)
                    ).first()
                    
                    if blocked:
                        await websocket.send_json({
                            "type": "error",
                            "message": "You are blocked or you blocked this user"
                        })
                        continue
                    
                    existing = db.query(Message).filter(
                        Message.message_id == message_id
                    ).first()
                    
                    if existing:
                        await websocket.send_json({
                            "type": "error",
                            "message": "Message ID already exists"
                        })
                        continue
                    
                    new_message = Message(
                        message_id=message_id,
                        send_id=send_id,
                        chat_id=chat_id,
                        sender_id=user.id,
                        content=content,
                        file_url=file_url,
                        file_type=file_type,
                        file_name=file_name,
                        created_at=datetime.utcnow()
                    )
                    db.add(new_message)
                    db.commit()
                    
                    response_message = {
                        "type": "message",
                        "message_id": message_id,
                        "send_id": send_id,
                        "sender_id": user.id,
                        "chat_id": chat_id,
                        "content": content,
                        "file_url": file_url,
                        "file_type": file_type,
                        "file_name": file_name,
                        "created_at": new_message.created_at.isoformat()
                    }
                    
                    members = db.query(ChatMember).filter(ChatMember.chat_id == chat_id).all()
                    for member in members:
                        if member.user_id != user.id:
                            await manager.send_personal_message(response_message, member.user_id)
                    
                elif message_type == "delivered":
                    message_id = message.get("message_id")
                    send_id = message.get("send_id")
                    
                    msg = db.query(Message).filter(
                        Message.message_id == message_id,
                        Message.send_id == send_id
                    ).first()
                    
                    if msg:
                        msg.delivered_at = datetime.utcnow()
                        db.commit()
                        
                        await manager.send_personal_message({
                            "type": "delivered",
                            "message_id": message_id,
                            "send_id": send_id
                        }, msg.sender_id)
                
                elif message_type == "read":
                    message_id = message.get("message_id")
                    send_id = message.get("send_id")
                    
                    msg = db.query(Message).filter(
                        Message.message_id == message_id,
                        Message.send_id == send_id
                    ).first()
                    
                    if msg:
                        msg.is_read = True
                        msg.read_at = datetime.utcnow()
                        db.commit()
                        
                        await manager.send_personal_message({
                            "type": "read",
                            "message_id": message_id,
                            "send_id": send_id,
                            "reader_id": user.id
                        }, msg.sender_id)
                
            except json.JSONDecodeError:
                await websocket.send_json({
                    "type": "error",
                    "message": "Invalid JSON format"
                })
            except Exception as e:
                logger.error(f"WebSocket message processing error: {e}")
                await websocket.send_json({
                    "type": "error",
                    "message": f"Error processing message: {str(e)}"
                })
    
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for user {user.id if user else 'unknown'}")
    except Exception as e:
        logger.error(f"WebSocket connection error: {e}")
    finally:
        if user:
            try:
                user.is_online = False
                user.last_seen = datetime.utcnow()
                db.commit()
                
                await manager.broadcast_to_chat({
                    "type": "status",
                    "user_id": user.id,
                    "is_online": False
                }, None, db)
            except:
                pass
        
        manager.disconnect(user.id if user else -1)

# ===========================================
# ЗАПУСК
# ===========================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
