"""
Telegram Channel & Group Monitor
Monitors ALL your public channels AND private groups.
Forwards unique (non-duplicate) messages to your own channel.

Setup instructions: See SETUP_GUIDE.md
"""

import os
import asyncio
import hashlib
import time
from difflib import SequenceMatcher
from telethon import TelegramClient, events, utils
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Chat

# ============================================================
# CONFIGURATION - Set these as environment variables in Railway
# ============================================================
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
TARGET_CHANNEL = os.environ.get("TARGET_CHANNEL", "")  # e.g. @my_filtered_news
SIMILARITY_THRESHOLD = float(os.environ.get("SIMILARITY_THRESHOLD", "0.75"))

# How many recent messages to remember for duplicate checking
MAX_HISTORY = 500

# ============================================================
# DUPLICATE DETECTION
# ============================================================
message_history = []  # List of recent message hashes/texts


def normalize_text(text):
    """Clean up text for comparison"""
    if not text:
        return ""
    # Remove extra whitespace, lowercase
    return " ".join(text.lower().split())


def is_duplicate(new_text):
    """Check if a message is too similar to recent messages"""
    if not new_text or len(new_text.strip()) < 10:
        return False  # Skip very short messages

    normalized = normalize_text(new_text)

    # Check exact hash match first (fastest)
    text_hash = hashlib.md5(normalized.encode()).hexdigest()
    for entry in message_history:
        if entry["hash"] == text_hash:
            return True

    # Check fuzzy similarity
    for entry in message_history:
        similarity = SequenceMatcher(None, normalized, entry["text"]).ratio()
        if similarity >= SIMILARITY_THRESHOLD:
            return True

    # Not a duplicate — add to history
    message_history.append({
        "hash": text_hash,
        "text": normalized,
        "time": time.time()
    })

    # Keep history from growing too large
    if len(message_history) > MAX_HISTORY:
        message_history.pop(0)

    return False


# ============================================================
# MAIN BOT
# ============================================================
async def main():
    # Validate configuration
    if not all([API_ID, API_HASH, SESSION_STRING, TARGET_CHANNEL]):
        print("\n❌ ERROR: Missing configuration!")
        print("Please set these environment variables in Railway:")
        print("  - API_ID")
        print("  - API_HASH")
        print("  - SESSION_STRING")
        print("  - TARGET_CHANNEL")
        return

    # Connect using your session string
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.start()
    me = await client.get_me()
    print(f"✅ Connected as: {me.first_name} ({me.phone})")

    # --------------------------------------------------------
    # Find all channels and groups to monitor
    # --------------------------------------------------------
    print("\n📡 Finding your channels and groups...")

    monitored_ids = set()
    channel_count = 0
    group_count = 0

    # Get the target channel ID so we don't monitor our own output
    try:
        target_entity = await client.get_entity(TARGET_CHANNEL)
        # Use marked ID (same format as event.chat_id)
        target_id = utils.get_peer_id(target_entity)
    except Exception:
        target_id = None

    async for dialog in client.iter_dialogs():
        entity = dialog.entity

        # Use marked peer ID — this matches what event.chat_id returns
        marked_id = utils.get_peer_id(entity)

        # Public channels (broadcast channels you follow)
        if isinstance(entity, Channel) and entity.broadcast:
            if marked_id != target_id:
                monitored_ids.add(marked_id)
                print(f"  📺 Channel: {dialog.name}")
                channel_count += 1

        # Groups (megagroups / supergroups / private groups)
        elif isinstance(entity, Channel) and entity.megagroup:
            if marked_id != target_id:
                monitored_ids.add(marked_id)
                print(f"  👥 Group: {dialog.name}")
                group_count += 1

        # Old-style small groups
        elif isinstance(entity, Chat):
            monitored_ids.add(marked_id)
            print(f"  👥 Group: {dialog.name}")
            group_count += 1

    print(f"\n📊 Monitoring {channel_count} channels + {group_count} groups = {channel_count + group_count} total")

    # --------------------------------------------------------
    # Listen for new messages
    # --------------------------------------------------------
    @client.on(events.NewMessage())
    async def handler(event):
        try:
            # Only process messages from monitored chats
            chat_id = event.chat_id
            if chat_id not in monitored_ids:
                return

            # Get chat name for the header
            try:
                chat = await event.get_chat()
                chat_name = getattr(chat, 'title', 'Unknown')
            except Exception:
                chat_name = "Unknown"

            # Get message text
            message_text = event.message.text or ""

            # Skip empty or very short messages
            if not message_text or len(message_text.strip()) < 5:
                return

            # Check for duplicates
            if is_duplicate(message_text):
                print(f"  🔁 [{chat_name}] Duplicate skipped")
                return

            # Forward unique message!
            print(f"  ✅ [{chat_name}] Unique! Forwarding...")
            try:
                await client.forward_messages(TARGET_CHANNEL, event.message)
                print(f"  📤 Forwarded successfully\n")
            except Exception as e:
                print(f"  ⚠️ Forward failed: {e}")
                # Try sending as text instead
                try:
                    chat_entity = await event.get_chat()
                    if isinstance(chat_entity, Channel) and chat_entity.broadcast:
                        emoji = "📺"
                    else:
                        emoji = "👥"

                    header = f"{emoji} **{chat_name}**\n\n"
                    await client.send_message(
                        TARGET_CHANNEL,
                        header + message_text,
                        link_preview=False
                    )
                    print(f"  📤 Sent as text instead\n")
                except Exception as e2:
                    print(f"  ❌ Also failed to send as text: {e2}\n")

        except Exception as e:
            print(f"  ❌ Error handling message: {e}\n")

    print("\n🎧 Listening for new messages from all channels & groups...\n")

    # Keep running forever
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
