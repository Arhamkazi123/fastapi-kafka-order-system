from sqlmodel import SQLModel,Field
from typing import Optional

class UserBase(SQLModel):
	username:str=Field(unique=True, index=True, min_length=3, max_length=50)
	
class UserCreate(UserBase):
	password:str=Field(min_length=8, max_length=50)

class UserRead(UserBase):
	id:int

class User(UserBase,table=True):
	id: Optional[int] = Field(default=None, primary_key=True)
	hashed_password: str
