from sqlmodel import create_engine,Session


sql_filename="database.db"
sql_url=f"sqlite:///{sql_filename}"
engine=create_engine(sql_url,connect_args={"check_same_thread": False})

def get_session():
	with Session(engine) as session:
		yield session