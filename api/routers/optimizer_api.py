# api/optimizer_api.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from api.database import get_db
from api.models import Service
from frequency_optimiser import FrequencyOptimizer  # Your existing optimizer

router = APIRouter(prefix="/api/optimizer", tags=["Optimizer"])

@router.post("/run/{service_id}")
async def run_optimization(
    service_id: int, 
    db: Session = Depends(get_db)
):
    """Independent optimizer endpoint"""
    try:
        # 1. Get service config from DB
        service = db.query(Service).get(service_id)
        if not service:
            raise HTTPException(404, "Service not found")
        
        # 2. Run optimization (bi-directional DB access)
        optimizer = FrequencyOptimizer(db)
        results = optimizer.run(service_id)
        
        # 3. Save back to DB
        service.optimization_parameters = results
        db.commit()
        
        return {"status": "success", "results": results}
    
    except Exception as e:
        db.rollback()
        raise HTTPException(500, str(e))