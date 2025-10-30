###############################################################################
# Entrypoint for the WhatsApp Chatbot Webhook Core Functionalities
###############################################################################

# Built-in imports
import os

# External imports
from mangum import Mangum
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse

# Own imports
from whatsapp_webhook.api.v1.routers import webhook

# Environment used to dynamically load the FastAPI docs with stages
ENVIRONMENT = os.environ.get("ENVIRONMENT")
API_PREFIX = "/api/v1"


app = FastAPI(
    title="WhatsApp Chatbot API",
    description="Custom built API by Santi to interact with the WhatsApp Chatbot",
    version="v1",
    root_path=f"/{ENVIRONMENT}" if ENVIRONMENT else None,
    docs_url="/api/v1/docs",
    openapi_url="/api/v1/docs/openapi.json",
)


# --- WhatsApp Webhook GET verification (Meta subscription check) ---
@app.get(f"{API_PREFIX}/webhook")
async def whatsapp_verify(request: Request):
    """
    Meta calls this with:
      hub.mode=subscribe
      hub.verify_token=<token>
      hub.challenge=<random string to echo>
    We must return 200 and the raw challenge string if the token matches.
    """
    verify_token = os.environ.get("VERIFY_TOKEN", "my-whatsapp-bot-verify-123")
    qs = request.query_params

    mode = qs.get("hub.mode")
    token = qs.get("hub.verify_token")
    challenge = qs.get("hub.challenge")

    if mode == "subscribe" and token == verify_token and challenge:
        return PlainTextResponse(str(challenge), status_code=200)

    return PlainTextResponse("Forbidden", status_code=403)


# --- Existing webhook router (likely handles POST /api/v1/webhook) ---
app.include_router(webhook.router, prefix=API_PREFIX)

# This is the Lambda Function's entrypoint (handler)
handler = Mangum(app)
