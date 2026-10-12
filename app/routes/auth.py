from fastapi import APIRouter,Depends,HTTPException,status
from sqlmodel import select
from app.database import get_session
from app.models import UserCreate,User,UserRead,UserBase
from app.auth import hash_password,create_access_token,verify_password

router=APIRouter(prefix="/auth",tags=["auth"])


@router.post("/register",status_code=status.HTTP_201_CREATED,response_model=UserRead)
def register_user(user_details:UserCreate,session=Depends(get_session)):

	statement = select(User).where(User.username == user_details.username)
	existing_user = session.exec(statement).first()

	if existing_user:
		raise HTTPException(
			status_code=status.HTTP_400_BAD_REQUEST,
			detail="username already exists"
		)

	hashed_pwd=hash_password(user_details.password)
	
	db_user=User(username=user_details.username, hashed_password=hashed_pwd)

	session.add(db_user)
	session.commit()
	session.refresh(db_user)

	return db_user



@router.post("/login")
def login_user(user_details:UserCreate,session=Depends(get_session)):
	statement=select(User).where(User.username==user_details.username)
	existing_user = session.exec(statement).first()

	if not existing_user or not verify_password(user_details.password,existing_user.hashed_password):
		raise HTTPException(
			status_code=status.HTTP_401_UNAUTHORIZED,
			detail="Invalid username or password"
		)

	access_token = create_access_token(data={"sub": existing_user.username})

	return {"access_token": access_token, "token_type": "bearer"}