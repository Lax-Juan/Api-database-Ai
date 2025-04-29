# app.py
from fastapi import FastAPI, Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader
from pydantic import BaseModel
from typing import List, Dict, Any
import psycopg2
from psycopg2 import pool, sql, errors
import os
from dotenv import load_dotenv
import logging
from fastapi.middleware.cors import CORSMiddleware

# Configuración inicial
load_dotenv(override=True)
# Logger configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# FastAPI app setup
app = FastAPI(
    title="Lax Data API",
    description="API para gestión de datos de Lax Tech",
    version="1.0.0",
    docs_url="/docs",
    redoc_url=None
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security setup
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

# Pydantic models
class QueryRequest(BaseModel):
    query: str

class QueryResponse(BaseModel):
    query: str
    results: List[Dict[str, Any]]

# Database configuration
DATABASE_URI = os.getenv("DATABASE_URI")
MIN_CONNECTIONS = 1
MAX_CONNECTIONS = 10

# Connection pool setup
try:
    connection_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=MIN_CONNECTIONS,
        maxconn=MAX_CONNECTIONS,
        dsn=DATABASE_URI,
        sslmode="require"
    )
    logger.info("Database connection pool created successfully")
except psycopg2.OperationalError as e:
    logger.error(f"Error creating connection pool: {str(e)}")
    raise RuntimeError("Database connection failed") from e

# Security dependencies
async def get_api_key(api_key: str = Security(api_key_header)):
    if api_key != os.getenv("API_SECRET_KEY"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API Key"
        )
    return api_key

@app.post("/query", response_model=QueryResponse)
async def execute_query(
    request: QueryRequest,
    api_key: str = Depends(get_api_key)
):
    """
    Execute a SELECT query against the PostgreSQL database
    
    - **query**: Valid SQL SELECT query
    """
    try:
        # Query validation
        clean_query = request.query.strip().lower()
        if not clean_query.startswith('select'):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only SELECT queries are allowed"
            )

        # Get connection from pool
        conn = connection_pool.getconn()
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL(request.query))
            
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                results = cursor.fetchall()
                
                # Transformar resultados a lista de diccionarios
                formatted_results = [
                    dict(zip(columns, row))
                    for row in results
                ]
                
                return {
                    "query": request.query,
                    "results": formatted_results
                }

            return {"query": request.query, "results": []}

    except errors.UndefinedTable as e:
        logger.error(f"Table error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Table does not exist: {str(e)}"
        )
    except errors.SyntaxError as e:
        logger.error(f"Syntax error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"SQL syntax error: {str(e)}"
        )
    except psycopg2.Error as e:
        logger.error(f"Database error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )
    finally:
        if 'conn' in locals():
            connection_pool.putconn(conn)

@app.on_event("shutdown")
def shutdown_event():
    """Close connection pool on shutdown"""
    if connection_pool:
        connection_pool.closeall()
    logger.info("Database connection pool closed")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        ssl_keyfile=os.getenv("SSL_KEYFILE", None),
        ssl_certfile=os.getenv("SSL_CERTFILE", None)
    )