import uvicorn
from db.initialize_db import initialize_db
from elt.run_elt import run_elt

def run_fastapi_app():
    app_module = "api.app"
    host = "127.0.0.1"  
    port = 8000  
    
    uvicorn.run(app_module, host=host, port=port)

def main():
    initialize_db()
    run_elt()
    run_fastapi_app()

if __name__ == "__main__":
    main()