import uvicorn
from app.webapi import *

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    uvicorn.run(
        "app:app",
        host='localhost',
        port=settings.web_api.port,
        reload=True
    )
