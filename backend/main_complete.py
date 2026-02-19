"""
Buddyliko Main API
FastAPI application entry point
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import routers
from backend.api.auth import router as auth_router
from backend.api.admin import router as admin_router
from backend.api.organizations import router as org_router
from backend.api.billing import router as billing_router
from backend.api.notifications import router as notif_router
from backend.api.admin import router as admin_router
from backend.api.billing import router as billing_router
from backend.api.notifications import router as notifications_router

# Create FastAPI app
app = FastAPI(
    title="Buddyliko API",
    description="Data Mapping & Transformation Platform",
    version="2.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# CORS Configuration
origins = os.getenv('CORS_ORIGINS', 'http://localhost:8000,http://localhost:3000').split(',')

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth_router)
app.include_router(admin_router)
app.include_router(org_router)
app.include_router(billing_router)
app.include_router(notif_router)
app.include_router(admin_router)
app.include_router(billing_router)
app.include_router(notifications_router)

# Root endpoint
@app.get("/")
def root():
    return {
        "message": "Buddyliko API v2.0",
        "status": "running",
        "docs": "/api/docs"
    }

# Health check
@app.get("/api/health")
def health_check():
    return {
        "status": "healthy",
        "version": "2.0.0"
    }

# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "error": "Internal server error",
            "detail": str(exc) if os.getenv('ENV') == 'development' else "An error occurred"
        }
    )

# Startup event
@app.on_event("startup")
async def startup_event():
    print("🚀 Buddyliko API starting up...")
    print(f"📊 Environment: {os.getenv('ENV', 'development')}")
    print(f"🔐 JWT Token Expiry: {os.getenv('ACCESS_TOKEN_EXPIRE_MINUTES', '10080')} minutes")

# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    print("👋 Buddyliko API shutting down...")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "backend.api.main:app",
        host="0.0.0.0",
        port=8080,
        reload=True
    )
