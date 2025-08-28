# Android Ingestion Job - Local/Dev Execution

1. Export `GOOGLE_APPLICATION_CREDENTIALS` in your terminal session.

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/home/user/path-to-credentials
```

2. Configure the `.env` file as in `.env.example`

```ini
SERPAPI_KEY=your-serpapi-key
RUNTIME=dev # make sure it is set to `dev`
```

3. Run the following command to export `.env` to terminal session:

```bash
export $(cat .env | xargs)
```

4. Run the script locally. Make sure to use a Python `venv`
with the dependencies specified at the project root folder's `requirements.txt`.

```bash
python ios_ingestion.py
```