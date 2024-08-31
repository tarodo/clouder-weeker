from src.bp_adapter import collect_releases
from src.common import ReleaseMeta
from src.logging_config import setup_logging
from environs import Env

setup_logging()


BP_BASE_URL = "https://api.beatport.com"
RELEASES_URL = f"{BP_BASE_URL}/v4/catalog/releases"

if __name__ == "__main__":
    env = Env()
    env.read_env()
    bp_token = env.str("BP_TOKEN")
    release_attr = ReleaseMeta(week=21, year=2023, style_id=1)
    collect_releases(release_attr, RELEASES_URL, bp_token)
    print("Hello, World!")