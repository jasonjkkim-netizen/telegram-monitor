"""
Telegram Channel Monitor - Reads all your public channels and forwards
unique (non-duplicate) messages to your own channel in real-time.

Setup instructions: See SETUP_GUIDE.md
"""

import os
import asyncio
import hashlib
import time
from difflib import SequenceMatcher
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import Channel

# ============================================================
# CONFIGURATION - Set these as environment variables
# ============================================================
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")  # Login session as text string
TARGET_CHANNEL = os.environ.get("TARGET_CHANNEL", "")  # Your channel username like @mychannel
SIMILARITY_THRESHOLD = float(os.environ.get("SIMILARITY_THRESHOLD", "0.75"))

# ============================================================
# DUPLICATE DETECTION
# ============================================================
class DuplicateDetector:
    """Keeps track of recent messages and checks for duplicates."""

    def __init__(self, max_history=2000, similarity_threshold=0.75):
        self.history = []  # List of (text_hash, text_snippet, timestamp)
        self.max_history = max_history
        self.similarity_threshold = similarity_threshold

    def _clean_text(self, text):
        """Normalize text for comparison."""
        if not text:
            return ""
        # Remove extra whitespace, convert to lowercase
        text = " ".join(text.lower().split())
        # Remove common prefixes like forwarded tags
        for prefix in ["forwarded from", "via @", "🔔", "⚡", "🚨", "📢", "📣"]:
            text = text.replace(prefix, "")
        return text.strip()

    def _get_hash(self, text):
        """Create a hash of the cleaned text."""
        return hashlib.md5(text.encode("utf-8")).hexdigest()

    def is_duplicate(self, text):
        """
        Check if a message is a duplicate of a recent message.
        Returns True if duplicate, False if unique.
        """
        if not text or len(text.strip()) < 20:
            # Very short messages are hard to deduplicate meaningfully
            return False

        cleaned = self._clean_text(text)
        text_hash = self._get_hash(cleaned)

        # First check: exact match (fast)
        for stored_hash, stored_text, _ in self.history:
            if stored_hash == text_hash:
                print(f"  ⚡ Exact duplicate detected")
                return True

        # Second check: similarity match (slower but catches paraphrased content)
        for stored_hash, stored_text, _ in self.history:
            # Only compare texts of similar length (optimization)
            if abs(len(stored_text) - len(cleaned)) / max(len(stored_text), len(cleaned), 1) > 0.5:
                continue

            similarity = SequenceMatcher(None, cleaned[:500], stored_text[:500]).ratio()
            if similarity >= self.similarity_threshold:
                print(f"  🔍 Similar duplicate detected ({similarity:.0%} match)")
                return True

        # Not a duplicate - add to history
        self.history.append((text_hash, cleaned, time.time()))

        # Clean up old entries (keep only recent ones)
        if len(self.history) > self.max_history:
            self.history = self.history[-self.max_history:]

        return False


# ============================================================
# MAIN BOT
# ============================================================
async def main():
    print("=" * 60)
    print("  Telegram Channel Monitor - Starting Up")
    print("=" * 60)

    # Validate configuration
    if not all([API_ID, API_HASH, SESSION_STRING, TARGET_CHANNEL]):
        print("\n❌ ERROR: Missing configuration!")
        print("Please set these environment variables:")
        print("  - API_ID")
        print("  - API_HASH")
        print("  - SESSION_STRING")
        print("  - TARGET_CHANNEL")
        return

    # Connect using your session string (no file needed!)
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.start()
    me = await client.get_me()
    print(f"✅ Connected as: {me.first_name} ({me.phone})")

    # Get all channels you follow (automatic!)
    print("\n📡 Finding your channels...")
    channels = []
    async for dialog in client.iter_dialogs():
        if isinstance(dialog.entity, Channel) and dialog.entity.broadcast:
            channels.append(dialog.entity)
            print(f"  📺 {dialog.name}")

    print(f"\n📊 Monitoring {len(channels)} channels")
    print(f"📤 Forwarding unique posts to: {TARGET_CHANNEL}")
    print(f"🔍 Duplicate threshold: {SIMILARITY_THRESHOLD:.0%}")
    print("\n" + "=" * 60)
    print("  Listening for new messages... (Press Ctrl+C to stop)")
    print("=" * 60 + "\n")

    # Initialize duplicate detector
    detector = DuplicateDetector(similarity_threshold=SIMILARITY_THRESHOLD)

    # Get channel IDs for event filtering
    channel_ids = [ch.id for ch in channels]

    @client.on(events.NewMessage(chats=channel_ids))
    async def handler(event):
        """Handle new messages from monitored channels."""
        try:
            # Get channel info
            chat = await event.get_chat()
            channel_name = getattr(chat, "title", "Unknown")
            message_text = event.message.text or event.message.message or ""

            # For messages with media but no text, use caption
            if not message_text and event.message.media:
                message_text = "[Media content]"

            print(f"📨 New post from: {channel_name}")

            # Check for duplicates
            if detector.is_duplicate(message_text):
                print(f"  ❌ Skipped (duplicate)\n")
                return

            # Not a duplicate! Forward it
            print(f"  ✅ Unique! Forwarding to {TARGET_CHANNEL}")
            try:
                await client.forward_messages(
                    TARGET_CHANNEL,
                    event.message
                )
                print(f"  📤 Forwarded successfully\n")
            except Exception as e:
                print(f"  ⚠️ Forward failed: {e}")
                # Try sending as text instead
                try:
                    header = f"📺 **{channel_name}**\n\n"
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

    # Keep running
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
