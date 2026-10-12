from fastapi import APIRouter
from app.routes.auth import router as auth_router
from app.routes.orders import router as orders_router
from app.routes.admin import router as admin_router

router = APIRouter()
router.include_router(auth_router)
router.include_router(orders_router)
router.include_router(admin_router)