# Kikoba Manager (local)

## Requirements
- Python 3.9+
- pip

## Backend setup
1. cd backend
2. python -m venv venv
3. source venv/bin/activate  # Windows: venv\\Scripts\\activate
4. pip install -r requirements.txt
5. python init_db.py   # creates backend/kikoba.db and seeds loan brackets & settings
6. python app.py       # runs Flask server at http://127.0.0.1:5000

## Frontend
1. cd frontend
2. serve the folder via a local HTTP server (so service worker works)
   - python -m http.server 8000
3. Open http://127.0.0.1:8000 in your browser.

Notes:
- The frontend expects the backend at http://127.0.0.1:5000/api
- If running backend on different port/origin, update `API` URL in `frontend/main.js`.
