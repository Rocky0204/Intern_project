# api/emulator_api.py  
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from api.database import get_db
from api.models import Block
from bus_simulation import BusEmulator  # Your existing emulator

router = APIRouter(prefix="/api/emulator", tags=["Emulator"])

@router.post("/run/{block_id}")
async def run_simulation(
    block_id: int,
    db: Session = Depends(get_db)
):
    """Independent emulator endpoint"""
    try:
        # 1. Get block data from DB
        block = db.query(Block).get(block_id)
        if not block:
            raise HTTPException(404, "Block not found")
        
        # 2. Run simulation (bi-directional DB access)
        emulator = BusEmulator(db)
        results = emulator.run(block_id)
        
        # 3. Save back to DB
        block.simulation_data = results
        db.commit()
        
        return {"status": "success", "metrics": results}
    
    except Exception as e:
        db.rollback()
        raise HTTPException(500, str(e))