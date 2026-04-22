"""Catalyst File Store helpers — stash the SQLite DB as a blob so it survives
between Function invocations and AppSail restarts.

Gracefully no-ops when DB_FOLDER_ID isn't set or the SDK isn't
installed — that path is for local development where SQLite just lives on disk.
"""
import logging
import os

DB_PATH = os.environ.get("DB_PATH", "scrapper.db")
FOLDER_ID = os.environ.get("DB_FOLDER_ID", "").strip()
FILE_NAME = os.environ.get("CATALYST_DB_FILENAME", "scrapper.db")

_log = logging.getLogger(__name__)


def _get_folder():
    if not FOLDER_ID:
        return None
    try:
        import zcatalyst_sdk
    except ImportError:
        _log.info("zcatalyst_sdk not installed; skipping File Store")
        return None
    try:
        app = zcatalyst_sdk.initialize()
        return app.filestore().folder(FOLDER_ID)
    except Exception as exc:  # noqa: BLE001
        _log.warning("Catalyst SDK init failed: %s", exc)
        return None


def download_db():
    """Pull scrapper.db from File Store to DB_PATH. Returns True if downloaded,
    False on first run (file not yet created) or when SDK unavailable."""
    folder = _get_folder()
    if folder is None:
        return False
    try:
        files = folder.get_files()
        target = next((f for f in files if getattr(f, "name", None) == FILE_NAME), None)
        if not target:
            _log.info("no %s in File Store yet — first run", FILE_NAME)
            return False
        data = target.download()
        with open(DB_PATH, "wb") as fp:
            fp.write(data if isinstance(data, (bytes, bytearray)) else data.read())
        _log.info("downloaded %s → %s", FILE_NAME, DB_PATH)
        return True
    except Exception as exc:  # noqa: BLE001
        _log.warning("download_db failed, continuing with local DB: %s", exc)
        return False


def upload_db():
    """Push DB_PATH up to File Store, replacing any existing copy."""
    folder = _get_folder()
    if folder is None or not os.path.exists(DB_PATH):
        return False
    try:
        for f in folder.get_files():
            if getattr(f, "name", None) == FILE_NAME:
                f.delete()
                break
        folder.upload_file(file_path=DB_PATH, name=FILE_NAME)
        _log.info("uploaded %s to File Store", DB_PATH)
        return True
    except Exception as exc:  # noqa: BLE001
        _log.error("upload_db failed: %s", exc)
        return False
