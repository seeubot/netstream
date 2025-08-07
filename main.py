# main_app.py

import os
import json
import pymongo
import telegram
from flask import Flask, request, jsonify
from waitress import serve
from dotenv import load_dotenv

# Load environment variables from a .env file
# Ensure you have a .env file with MONGO_URI, TELEGRAM_BOT_TOKEN, and WEBHOOK_URL
load_dotenv()

# --- Configuration & Initialization ---
app = Flask(__name__)

# Get secrets from environment variables
MONGO_URI = os.getenv("MONGO_URI")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

# Global variables for the database and bot client
db_client = None
db = None
content_collection = None
bot = None

# --- MongoDB Connection Function ---
def connect_to_mongodb():
    """Establishes and tests the connection to the MongoDB database."""
    global db_client, db, content_collection
    try:
        db_client = pymongo.MongoClient(MONGO_URI)
        db_client.admin.command('ping')  # The 'ping' command tests the connection
        db = db_client.get_database("your_database_name") # Replace with your database name
        content_collection = db.get_collection("your_collection_name") # Replace with your collection name
        print("✅ MongoDB connected successfully!")
        return True
    except pymongo.errors.ConnectionFailure as e:
        print(f"❌ Failed to connect to MongoDB: {e}")
        db_client = None
        return False
    except Exception as e:
        print(f"❌ An unexpected error occurred during MongoDB connection: {e}")
        db_client = None
        return False

# --- Telegram Bot Webhook Setup ---
def setup_telegram_webhook():
    """Initializes the bot and sets the webhook URL."""
    global bot
    if not BOT_TOKEN or not WEBHOOK_URL:
        print("❌ BOT_TOKEN or WEBHOOK_URL is not set. Cannot configure webhook.")
        return

    bot = telegram.Bot(token=BOT_TOKEN)
    full_webhook_url = f"{WEBHOOK_URL}/{BOT_TOKEN}"

    try:
        # The user's original log showed a ConnectionResetError, which is often
        # a network issue or an invalid URL/token. This try-except block
        # makes the webhook setup more resilient.
        is_webhook_set = bot.set_webhook(url=full_webhook_url)
        if is_webhook_set:
            print(f"✅ Webhook set successfully to: {full_webhook_url}")
        else:
            print("❌ Failed to set webhook, but no exception was raised.")
    except Exception as e:
        # This catches any errors during the webhook setup, preventing a crash.
        print(f"❌ Failed to set webhook due to an error: {e}")

# --- Fixed get_content_library Function ---
def get_content_library():
    """
    Fetches content from the MongoDB collection.
    
    FIX: The original error `Collection objects do not implement truth value testing`
    was caused by a line like `if content_collection:`. The fix is to
    explicitly compare the object to `None`, as PyMongo collection objects
    cannot be used in a boolean context directly.
    """
    # This is the corrected line.
    if content_collection is not None:
        try:
            print("Fetching from content_library...")
            # Example: Fetching all documents
            content_list = list(content_collection.find({}))
            return content_list
        except Exception as e:
            print(f"❌ Error in get_content_library: {e}")
            return []
    else:
        # This branch handles the case where the collection was not initialized
        # because the database connection failed.
        print("❌ Error in get_content_library: MongoDB collection is not available.")
        return []

# --- Flask Routes ---
@app.route('/')
def home():
    """Simple home route to indicate the server is running."""
    return "Application is running and healthy."

@app.route(f"/{BOT_TOKEN}", methods=['POST'])
def telegram_webhook_handler():
    """Handles incoming updates from Telegram."""
    if not bot:
        print("❌ Bot is not initialized.")
        return jsonify({"status": "error", "message": "Bot not initialized."}), 500

    try:
        update = telegram.Update.de_json(request.get_json(force=True), bot)
        # Placeholder for your bot's logic to process the update
        print(f"Received update from Telegram: {update.update_id}")
        return 'ok'
    except Exception as e:
        print(f"❌ Error processing Telegram update: {e}")
        return 'error'

# --- Main Application Entry Point ---
if __name__ == '__main__':
    # Connect to the database and set up the bot before starting the server
    if connect_to_mongodb():
        setup_telegram_webhook()
    
    # Use Waitress for serving the Flask application in production
    print("Starting Flask application with Waitress...")
    # The user's log indicates this part is already working.
    serve(app, host='0.0.0.0', port=5000)

