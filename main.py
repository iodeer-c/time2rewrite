from fastapi import FastAPI

app = FastAPI(title="Time Query Service")


@app.get("/")
def root() -> dict[str, str]:
    return {"message": "time-query-service"}
