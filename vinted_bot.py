"""
╔═══════════════════════════════════════════════════════╗
║   VINTED BOT v6 — architecture asynchrone             ║
║   Recherches concurrentes · envoi Discord découplé    ║
║   Planification par échéance · dashboard SSE          ║
╚═══════════════════════════════════════════════════════╝

Installation :
    pip install "curl_cffi>=0.7" colorama discord.py

Lancement :
    python vinted_bot.py

Dashboard :
    http://localhost:8765
"""

# ── Dépendances ────────────────────────────────────────────────────
import sys

def _check_deps():
    missing = []
    for pkg, mod in (("curl_cffi", "curl_cffi"), ("colorama", "colorama"), ("discord.py", "discord")):
        try:
            __import__(mod)
        except ImportError:
            missing.append(pkg)
    if missing:
        print(f"\n[ERREUR] Dépendances manquantes : {', '.join(missing)}")
        print(f"Lance : pip install {' '.join(missing)}\n")
        sys.exit(1)

_check_deps()

import asyncio
import json
import os
import random
import re
import signal
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

import discord
from curl_cffi.requests import AsyncSession
from colorama import init as colorama_init, Fore, Style

colorama_init(autoreset=True)


# ═══════════════════════════════════════════════════════════════════
#  CONSTANTES
# ═══════════════════════════════════════════════════════════════════
VINTED_BASE = "https://www.vinted.fr"
# Confirmé par capture réseau réelle (DevTools) le 2026-09-29 : le site
# n'appelle PAS www.vinted.fr/api/v2/catalog/items (ancien/mauvais endpoint,
# renvoie un 404 statique quels que soient les paramètres — c'était la
# cause de tous les échecs précédents). Le vrai appel va sur un sous-domaine
# dédié avec un chemin et un schéma de paramètres différents.
API_BASE    = "https://api.vinted.fr"
SEARCH_URL  = f"{API_BASE}/svc-catalogue/items"

# Sur Railway, le système de fichiers est éphémère : tout ce qui n'est pas
# sur un Volume monté disparaît à chaque redéploiement. DATA_DIR pointe vers
# ce volume (ex: /data) en production ; en local, "." suffit (comportement
# inchangé pour un lancement sur ta machine).
DATA_DIR = Path(os.environ.get("DATA_DIR", "."))
DATA_DIR.mkdir(parents=True, exist_ok=True)

CONFIG_FILE = DATA_DIR / "config.json"
ANON_ID = str(uuid.uuid4())  # identifiant visiteur anonyme, requis par l'API
STATE_FILE  = DATA_DIR / "bot_state.json"
SEEN_FILE   = DATA_DIR / "seen_items.json"
DASH_FILE   = Path("dashboard.html")  # livré avec le code, pas sur le volume

BOT_VERSION = "v6.10"   # affiché au démarrage pour confirmer que le bon code tourne

# Railway assigne dynamiquement le port via la variable d'env PORT et route
# le trafic public vers celui-ci — on ne peut pas garder 8765 en dur.
DASH_PORT   = int(os.environ.get("PORT", 8765))
# En local (pas de PORT fourni), le dashboard reste accessible uniquement
# depuis la machine — pas de changement de comportement pour toi. Sur
# Railway, PORT est toujours présent : on écoute alors sur toutes les
# interfaces, seule façon pour Railway de router le trafic public jusqu'ici.
DASH_HOST   = "0.0.0.0" if "PORT" in os.environ else "127.0.0.1"
# chrome124 date d'avril 2024 — un profil TLS trop ancien peut lui-même être
# un signal pour un WAF qui compare la version prétendue à ce qui circule
# réellement. On utilise un profil plus récent disponible dans cette
# version de curl_cffi.
IMPERSONATE = "chrome136"

MAX_CONCURRENT_SEARCHES = 3      # recherches simultanées
MAX_SEEN_PER_SEARCH     = 3000   # IDs mémorisés par recherche
SESSION_TTL_SECONDS     = 900    # rafraîchit la session toutes les 15 min
STATE_FLUSH_INTERVAL    = 1.0    # écriture disque max 1×/s
DISCORD_SEND_DELAY      = 0.9    # délai entre deux envois Discord
JITTER_RATIO            = 0.12   # ±12 % sur les intervalles
MIN_INTERVAL            = 15.0   # plancher absolu, anti-ban
MAX_NOTIFY_PER_SCAN     = 10     # garde-fou anti-rafale par recherche

DEFAULT_CONFIG = {
    "discord_webhook_url": "COLLE_TON_WEBHOOK_ICI",
    "discord_bot_token":   "COLLE_TON_BOT_TOKEN_ICI",
    "searches": [
        {
            "name": "Exemple – mode URL",
            "url": "https://www.vinted.fr/catalog?search_text=jordan+1&price_to=120",
            "interval": 60,
            "enabled": True,
        }
    ],
    "poll_interval_seconds": 60,
    "proxies": {"enabled": False, "rotate_every": 3, "list": []},
}


# ═══════════════════════════════════════════════════════════════════
#  LOGS
# ═══════════════════════════════════════════════════════════════════
_LOG_STYLE = {
    "INFO":    (Fore.CYAN,                   "·"),
    "SUCCESS": (Fore.GREEN + Style.BRIGHT,   "✓"),
    "WARN":    (Fore.YELLOW,                 "⚠"),
    "ERROR":   (Fore.RED + Style.BRIGHT,     "✗"),
    "FOUND":   (Fore.MAGENTA + Style.BRIGHT, "★"),
}

def log(msg: str, level: str = "INFO"):
    color, prefix = _LOG_STYLE.get(level, (Fore.WHITE, "·"))
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"{Style.DIM}{ts}{Style.RESET_ALL}  {color}{prefix} {msg}")


# ═══════════════════════════════════════════════════════════════════
#  ÉTAT PARTAGÉ (dashboard + commandes Discord)
# ═══════════════════════════════════════════════════════════════════
class BotState:
    """
    État central du bot. Écrit sur disque de façon différée (debounce)
    pour éviter des centaines d'écritures par seconde.
    """
    MAX_LOGS  = 120
    MAX_ITEMS = 60

    def __init__(self):
        self.status       = "stopped"
        self.started_at   = None
        self.cycle        = 0
        self.last_cycle   = None
        self.proxy_status = "proxies désactivés"
        self.paused       = False
        self.stats        = {"found": 0, "sent": 0, "errors": 0, "requests": 0}
        self.searches     = []
        self.logs         = deque(maxlen=self.MAX_LOGS)
        self.items        = deque(maxlen=self.MAX_ITEMS)
        self.version      = 0       # incrémenté à chaque changement (SSE)
        self._dirty       = False
        self._lock        = threading.Lock()

    def touch(self):
        with self._lock:
            self.version += 1
            self._dirty = True

    def log(self, msg: str, level: str = "INFO"):
        log(msg, level)
        self.logs.append({
            "ts": datetime.now().strftime("%H:%M:%S"),
            "level": level,
            "msg": msg,
        })
        self.touch()

    def add_item(self, item: dict, search_name: str):
        self.items.appendleft({
            "id":        str(item.get("id", "")),
            "title":     item.get("title", ""),
            "brand":     get_brand(item),
            "size":      get_size(item),
            "price":     get_price(item),
            "url":       item.get("url") or f"{VINTED_BASE}/items/{item.get('id','')}",
            "image":     get_image(item),
            "seller":    (item.get("user") or {}).get("login", "—"),
            "condition": get_condition(item),
            "search":    search_name,
            "found_at":  datetime.now().strftime("%H:%M:%S"),
        })
        self.touch()

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "status":       self.status,
                "started_at":   self.started_at,
                "cycle":        self.cycle,
                "last_cycle":   self.last_cycle,
                "proxy_status": self.proxy_status,
                "paused":       self.paused,
                "stats":        dict(self.stats),
                "searches":     list(self.searches),
                "logs":         list(self.logs),
                "items":        list(self.items),
                "version":      self.version,
            }

    def flush_if_dirty(self):
        with self._lock:
            if not self._dirty:
                return
            self._dirty = False
        try:
            STATE_FILE.write_text(
                json.dumps(self.snapshot(), ensure_ascii=False, default=str),
                encoding="utf-8",
            )
        except Exception:
            pass


state = BotState()


# ═══════════════════════════════════════════════════════════════════
#  MÉMOIRE DES ANNONCES VUES (persistée, bornée)
# ═══════════════════════════════════════════════════════════════════
class SeenStore:
    """
    Mémorise les IDs déjà notifiés, par recherche.
    Persisté sur disque : un redémarrage ne renvoie pas d'anciennes annonces.
    Borné : évite une fuite mémoire sur plusieurs jours de fonctionnement.
    """
    def __init__(self, path: Path, max_per_search: int):
        self.path = path
        self.max  = max_per_search
        self._data: dict[str, deque] = {}
        self._sets: dict[str, set]   = {}
        self._dirty = False
        self._load()

    def _load(self):
        if not self.path.exists():
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            for name, ids in raw.items():
                d = deque(ids[-self.max:], maxlen=self.max)
                self._data[name] = d
                self._sets[name] = set(d)
            total = sum(len(v) for v in self._data.values())
            log(f"{total} annonce(s) déjà connue(s) rechargée(s) depuis {self.path.name}", "SUCCESS")
        except Exception as e:
            log(f"Impossible de lire {self.path.name} : {e}", "WARN")

    def has(self, search: str, item_id: str) -> bool:
        return item_id in self._sets.get(search, ())

    def add(self, search: str, item_id: str):
        if search not in self._data:
            self._data[search] = deque(maxlen=self.max)
            self._sets[search] = set()
        d, s = self._data[search], self._sets[search]
        if len(d) == d.maxlen and d:
            s.discard(d[0])       # l'élément le plus ancien va être évincé
        d.append(item_id)
        s.add(item_id)
        self._dirty = True

    def count(self, search: str) -> int:
        return len(self._data.get(search, ()))

    def flush(self):
        if not self._dirty:
            return
        self._dirty = False
        try:
            self.path.write_text(
                json.dumps({k: list(v) for k, v in self._data.items()}, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception:
            pass


seen = SeenStore(SEEN_FILE, MAX_SEEN_PER_SEARCH)


# ═══════════════════════════════════════════════════════════════════
#  PROXIES ROTATIFS
# ═══════════════════════════════════════════════════════════════════
class ProxyManager:
    QUARANTINE = 300   # secondes

    def __init__(self, cfg: dict):
        self.enabled      = bool(cfg.get("enabled"))
        self.rotate_every = max(1, int(cfg.get("rotate_every", 3)))
        self.proxies      = [
            p.strip() for p in (cfg.get("list") or [])
            if p.strip() and "user:pass@ip" not in p
        ]
        self._idx   = 0
        self._count = 0
        self._dead: dict[str, float] = {}

        if self.enabled and not self.proxies:
            log("Proxies activés mais aucune entrée valide — désactivés", "WARN")
            self.enabled = False
        elif self.enabled:
            log(f"Proxies : {len(self.proxies)} entrée(s), rotation /{self.rotate_every} requête(s)", "SUCCESS")

    @staticmethod
    def mask(url: str) -> str:
        return re.sub(r"://[^@]+@", "://•••@", url or "")

    def _alive(self) -> list[str]:
        now = time.time()
        for url, ts in list(self._dead.items()):
            if now - ts > self.QUARANTINE:
                del self._dead[url]
                log(f"Proxy réhabilité : {self.mask(url)}", "INFO")
        return [p for p in self.proxies if p not in self._dead]

    def current(self) -> str | None:
        if not self.enabled:
            return None
        alive = self._alive()
        if not alive:
            return None
        return alive[self._idx % len(alive)]

    def tick(self):
        """Compte une requête et fait tourner si le seuil est atteint."""
        if not self.enabled:
            return
        self._count += 1
        if self._count >= self.rotate_every:
            self._count = 0
            alive = self._alive()
            if alive:
                self._idx = (self._idx + 1) % len(alive)

    def mark_dead(self, url: str | None):
        if url and url not in self._dead:
            log(f"Proxy en quarantaine 5 min : {self.mask(url)}", "WARN")
            self._dead[url] = time.time()

    def request_kwargs(self) -> dict:
        """kwargs à passer à une requête curl_cffi (rotation par requête)."""
        p = self.current()
        return {"proxies": {"http": p, "https": p}} if p else {}

    def status_line(self) -> str:
        if not self.enabled:
            return "proxies désactivés"
        alive = len(self._alive())
        return f"{alive}/{len(self.proxies)} proxy(s) actif(s) · courant : {self.mask(self.current() or 'direct')}"


proxy_mgr = ProxyManager({"enabled": False})


# ═══════════════════════════════════════════════════════════════════
#  EXTRACTION DES CHAMPS VINTED
# ═══════════════════════════════════════════════════════════════════
def get_price(item: dict) -> float:
    for key in ("price", "price_numeric", "total_item_price"):
        raw = item.get(key)
        if raw is None:
            continue
        if isinstance(raw, dict):
            raw = raw.get("amount") or raw.get("fractional") or 0
        try:
            v = float(str(raw).replace(",", "."))
            if v > 0:
                return v
        except (TypeError, ValueError):
            continue
    return 0.0

def _item_box_second_line_parts(item: dict) -> tuple[str, str]:
    """
    Découpe item_box['second_line'] (ex: "XS · Très bon état") en
    (taille, état). Confirmé par capture réelle le 2026-09-29 : c'est
    l'unique source de ces deux informations sur le nouvel endpoint,
    aucun des deux n'existe en top-level (brand_title/size_title/status
    étaient des clés de l'ANCIEN endpoint, absentes ici).
    """
    second = ((item.get("item_box") or {}).get("second_line") or "").strip()
    if not second:
        return "—", "—"
    parts = [p.strip() for p in second.split("·") if p.strip()]
    if len(parts) >= 2:
        return parts[0], " · ".join(parts[1:])
    if len(parts) == 1:
        # Une seule partie : les catégories sans taille (accessoires, déco...)
        # n'affichent que l'état — on la range donc en état, pas en taille.
        return "—", parts[0]
    return "—", "—"

def get_brand(item: dict) -> str:
    box_brand = (item.get("item_box") or {}).get("first_line")
    if box_brand and str(box_brand).strip():
        return str(box_brand).strip()
    return (item.get("brand_title") or (item.get("brand") or {}).get("title") or "—").strip() or "—"

def get_size(item: dict) -> str:
    if item.get("item_box"):
        size, _ = _item_box_second_line_parts(item)
        if size != "—":
            return size
    return (item.get("size_title") or (item.get("size") or {}).get("title") or item.get("size1_title") or "—").strip() or "—"

def get_condition(item: dict) -> str:
    if item.get("item_box"):
        _, condition = _item_box_second_line_parts(item)
        if condition != "—":
            return condition
    for key in ("status", "condition", "disposal_conditions"):
        raw = item.get(key)
        if not raw:
            continue
        if isinstance(raw, dict):
            return raw.get("title") or raw.get("name") or "—"
        if str(raw).strip():
            return str(raw).strip()
    return "—"

def get_image(item: dict) -> str | None:
    photos = item.get("photos") or ([item["photo"]] if item.get("photo") else [])
    if not photos or not isinstance(photos[0], dict):
        return None
    p = photos[0]
    for key in ("full_size_url", "url", "src", "image_url", "large_url", "medium_url"):
        v = p.get(key)
        if isinstance(v, str) and v.startswith("http"):
            return v
    for t in (p.get("thumbnails") or []):
        if isinstance(t, dict):
            v = t.get("url") or t.get("src")
            if isinstance(v, str) and v.startswith("http"):
                return v
    return None

def get_published(item: dict) -> str:
    for key in ("created_at_ts", "created_at", "updated_at", "last_lounge_at"):
        raw = item.get(key)
        if not raw:
            continue
        try:
            if isinstance(raw, (int, float)):
                dt = datetime.fromtimestamp(raw, tz=timezone.utc)
            else:
                dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
            return time_ago(dt)
        except Exception:
            continue
    return "—"

def time_ago(dt: datetime) -> str:
    secs = int((datetime.now(timezone.utc) - dt).total_seconds())
    if secs < 60:
        return "à l'instant"
    if secs < 3600:
        m = secs // 60
        return f"il y a {m} minute{'s' if m > 1 else ''}"
    if secs < 86400:
        h = secs // 3600
        return f"il y a {h} heure{'s' if h > 1 else ''}"
    d = secs // 86400
    return f"il y a {d} jour{'s' if d > 1 else ''}"


# ═══════════════════════════════════════════════════════════════════
#  CLIENT VINTED ASYNCHRONE
# ═══════════════════════════════════════════════════════════════════
class VintedClient:
    """
    Session async unique, partagée par toutes les recherches.
    Les cookies sont réutilisés ; la session n'est rafraîchie que
    sur échec réel ou après expiration du TTL.
    """
    def __init__(self):
        self.session: AsyncSession | None = None
        self._ready_at = 0.0
        self._lock = asyncio.Lock()

    async def open(self):
        self.session = AsyncSession(
            impersonate=IMPERSONATE,
            max_clients=MAX_CONCURRENT_SEARCHES * 2,
            timeout=20,
        )

    async def close(self):
        if self.session:
            try:
                await self.session.close()
            except Exception:
                pass

    @property
    def expired(self) -> bool:
        return time.time() - self._ready_at > SESSION_TTL_SECONDS

    async def ensure_session(self, force: bool = False) -> bool:
        """Établit ou renouvelle la session Vinted. Idempotent et thread-safe."""
        async with self._lock:
            if not force and not self.expired and self._ready_at > 0:
                return True

            proxy_mgr.tick()
            proxy = proxy_mgr.current()
            kwargs = proxy_mgr.request_kwargs()
            if proxy:
                state.log(f"Session via {ProxyManager.mask(proxy)}", "INFO")

            try:
                self.session.cookies.clear()
            except Exception:
                pass

            try:
                r = await self.session.get(
                    f"{VINTED_BASE}/catalog",
                    headers={"Accept-Language": "fr-FR,fr;q=0.9", "Referer": VINTED_BASE},
                    **kwargs,
                )
                state.stats["requests"] += 1
                await asyncio.sleep(0.8)

                probe = await self.session.get(
                    SEARCH_URL,
                    params={"per_page": 1, "page": 1},
                    headers=self._api_headers(),
                    **kwargs,
                )
                state.stats["requests"] += 1

                if "application/json" in probe.headers.get("Content-Type", ""):
                    self._ready_at = time.time()
                    state.log(f"Session Vinted opérationnelle ({len(self.session.cookies)} cookies)", "SUCCESS")
                    return True

                state.log(f"Vinted renvoie du HTML (statut {probe.status_code})", "WARN")
                proxy_mgr.mark_dead(proxy)
                return False

            except Exception as e:
                state.log(f"Échec session : {type(e).__name__} — {e}", "ERROR")
                proxy_mgr.mark_dead(proxy)
                return False

    @staticmethod
    def _api_headers() -> dict:
        # Corrigé le 2026-09-29 à partir d'une capture réseau réelle (DevTools)
        # d'un appel du navigateur vers api.vinted.fr/svc-catalogue/items.
        # Différences avec l'ancienne version, confirmées, pas supposées :
        #  - Referer est la racine du site (https://www.vinted.fr/), pas /catalog
        #  - Origin est requis (absent avant) : le vrai appel traverse un
        #    sous-domaine différent (www. → api.), donc Sec-Fetch-Site doit
        #    valoir "same-site" et non "same-origin" (ce n'est PAS la même
        #    origine au sens strict, juste le même site).
        #  - X-Requested-With n'apparaît PAS dans la vraie requête (convention
        #    jQuery obsolète que l'app moderne n'envoie plus) — retiré.
        #  - locale / platform / x-next-app sont des headers propres à l'API
        #    interne de Vinted, présents sur toute requête réelle observée.
        #  - x-anon-id identifie un visiteur anonyme ; généré aléatoirement
        #    ici (UUID), comme le ferait le JS du site au premier chargement.
        return {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "fr-FR,fr;q=0.9",
            "Referer": f"{VINTED_BASE}/",
            "Origin": VINTED_BASE,
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "locale": "fr-FR",
            "platform": "web",
            "x-anon-id": ANON_ID,
            "x-next-app": "marketplace-web",
        }

    @staticmethod
    def _build_params(cfg: dict) -> dict:
        """
        URL Vinted (copiée depuis la barre d'adresse du navigateur) →
        paramètres réels attendus par api.vinted.fr/svc-catalogue/items.

        IMPORTANT — confirmé par capture réseau réelle le 2026-09-29 :
        la page /catalog?... affichée dans le navigateur utilise des noms de
        paramètres (catalog[], size_ids[], brand_ids[], status_ids[]) qui NE
        SONT PAS ceux attendus par l'API réelle. L'API utilise un schéma
        `attribute_ids[xxx]` différent :
            catalog[]     → attribute_ids[catalog]
            size_ids[]    → attribute_ids[size]
            brand_ids[]   → attribute_ids[brand]
            status_ids[]  → attribute_ids[status]
            color_ids[]   → attribute_ids[color]      (non confirmé par capture)
            material_ids[]→ attribute_ids[material]   (non confirmé par capture)
        price_from/price_to/search_text restent des paramètres de premier
        niveau (pas de wrapping attribute_ids) — également confirmé.
        """
        ATTRIBUTE_MAP = {
            "catalog[]":      "attribute_ids[catalog]",
            "size_ids[]":     "attribute_ids[size]",
            "brand_ids[]":    "attribute_ids[brand]",
            "status_ids[]":   "attribute_ids[status]",
            "color_ids[]":    "attribute_ids[color]",
            "material_ids[]": "attribute_ids[material]",
            "pattern_ids[]":  "attribute_ids[patterns]",
        }
        PASSTHROUGH = {"price_from", "price_to", "search_text", "currency"}

        if cfg.get("url"):
            qs = parse_qs(urlparse(cfg["url"]).query, keep_blank_values=False)
            params: dict = {}
            for k, v in qs.items():
                value = v[0] if len(v) == 1 else v
                if k in ATTRIBUTE_MAP:
                    params[ATTRIBUTE_MAP[k]] = value
                elif k in PASSTHROUGH:
                    params[k] = value
                # les autres clés du navigateur (page, time, currency si non
                # voulu...) sont volontairement ignorées : elles sont soit
                # écrasées ci-dessous (page), soit propres au routage front
                # et sans effet sur l'API (time = cache-buster JS).
        else:
            params = {
                "search_text": cfg.get("keywords", ""),
                "price_from":  cfg.get("price_min") or "",
                "price_to":    cfg.get("price_max") or "",
            }
            params = {k: v for k, v in params.items() if v != ""}

        params.update({"order": "newest_first", "per_page": 48, "page": 1})
        return params

    async def search(self, cfg: dict) -> list[dict]:
        """Exécute une recherche. Renouvelle la session si nécessaire."""
        params = self._build_params(cfg)

        for attempt in (0, 1):
            if not await self.ensure_session(force=(attempt == 1)):
                if attempt == 1:
                    return []
                continue

            proxy_mgr.tick()
            proxy = proxy_mgr.current()

            try:
                r = await self.session.get(
                    SEARCH_URL,
                    params=params,
                    headers=self._api_headers(),
                    **proxy_mgr.request_kwargs(),
                )
                state.stats["requests"] += 1

                ct = r.headers.get("Content-Type", "")
                body_len = len(r.content or b"")

                # Cas 1 : statut ou content-type clairement invalides
                if "application/json" not in ct or r.status_code in (401, 403, 429):
                    if r.status_code == 429:
                        state.log("Vinted rate-limit (429) — ralentis l'intervalle", "WARN")
                    if r.status_code == 403:
                        proxy_mgr.mark_dead(proxy)
                    self._ready_at = 0
                    if attempt == 0:
                        continue
                    return []

                # Cas 2 : statut + content-type OK mais corps vide/invalide.
                # Observé en pratique : Vinted répond 200 + Content-Type
                # application/json mais un corps vide, plutôt qu'un blocage
                # HTML classique. Le renouvellement de session ne corrige
                # pas ce cas (la session est valide) — c'est vraisemblablement
                # un filtrage côté serveur sur cette requête précise.
                if body_len == 0:
                    state.log(
                        f"Corps vide malgré statut {r.status_code} et Content-Type JSON "
                        f"(params: {list(params.keys())[:4]}...)",
                        "WARN",
                    )
                    if attempt == 0:
                        # Petite pause avant de réessayer — évite de renvoyer
                        # immédiatement une requête identique
                        await asyncio.sleep(1.5)
                        continue
                    state.stats["errors"] += 1
                    return []

                try:
                    return r.json().get("items", [])
                except Exception as e:
                    # Ne devrait plus arriver grâce au check body_len==0 ci-dessus,
                    # mais on garde un filet de sécurité avec diagnostic complet.
                    preview = (r.content or b"")[:100]
                    state.log(
                        f"JSON invalide malgré statut {r.status_code}/{ct} "
                        f"({body_len} octets) : {e} — début du corps: {preview!r}",
                        "ERROR",
                    )
                    self._ready_at = 0
                    if attempt == 0:
                        continue
                    state.stats["errors"] += 1
                    return []

            except Exception as e:
                state.log(f"Erreur requête : {type(e).__name__} — {e}", "ERROR")
                proxy_mgr.mark_dead(proxy)
                self._ready_at = 0
                if attempt == 1:
                    state.stats["errors"] += 1
                    return []

        return []


vinted = VintedClient()


# ═══════════════════════════════════════════════════════════════════
#  FILTRAGE (mode classique uniquement — le mode URL délègue à Vinted)
# ═══════════════════════════════════════════════════════════════════
def normalize(text: str) -> str:
    return re.sub(r"[^a-z0-9 ]", "", (text or "").lower())

def passes_filters(item: dict, cfg: dict) -> bool:
    if cfg.get("url"):
        return True   # Vinted a déjà appliqué tous les filtres de l'URL

    title = normalize(item.get("title", ""))
    kw = cfg.get("keywords", "").strip()
    if kw and not all(w in title for w in normalize(kw).split()):
        return False

    brands = [b.strip() for b in (cfg.get("brands") or []) if b.strip()]
    if brands:
        bn = normalize(get_brand(item))
        if not any(normalize(b) in bn or bn in normalize(b) for b in brands):
            return False

    price = get_price(item)
    pmin, pmax = cfg.get("price_min"), cfg.get("price_max")
    if pmin is not None and 0 < price < float(pmin):
        return False
    if pmax is not None and price > float(pmax):
        return False
    return True


# ═══════════════════════════════════════════════════════════════════
#  ENVOI DISCORD (découplé via file d'attente)
# ═══════════════════════════════════════════════════════════════════
# Créée dans main(), à l'intérieur de la boucle asyncio, pour garantir
# qu'elle est liée à la bonne boucle quelle que soit la version de Python.
send_queue: asyncio.Queue = None  # type: ignore[assignment]

def _safe_url(url: str | None, fallback: str) -> str:
    """
    Garantit une URL absolue http(s) valide — Discord rejette (400) toute
    URL relative ou vide dans un embed OU un bouton de type lien.

    Le nouvel endpoint (api.vinted.fr/svc-catalogue/items) renvoie des
    chemins RELATIFS pour "url" (ex: "/items/123-sweat-nike"), pas des URLs
    complètes. On préfixe donc le domaine plutôt que de jeter ce chemin —
    ça préserve le slug SEO réel au lieu de reconstruire une URL plus pauvre.
    """
    if url and isinstance(url, str):
        if url.startswith(("http://", "https://")):
            return url
        if url.startswith("/"):
            return VINTED_BASE + url
    return fallback

def build_embed(item: dict, search_name: str) -> dict:
    price = get_price(item)
    # Confirmé par capture réelle : l'API renvoie le vrai total TTC
    # (total_item_price = price + service_fee), plus besoin d'estimer.
    total_raw = (item.get("total_item_price") or {}).get("amount")
    try:
        total = float(str(total_raw).replace(",", ".")) if total_raw else 0.0
    except (TypeError, ValueError):
        total = 0.0

    if price > 0 and total > 0:
        price_str = f"{price:.2f} EUR  |  {total:.2f} EUR TTC"
    elif price > 0:
        # Repli si total_item_price est absent pour une raison quelconque :
        # estimation approximative, clairement marquée comme telle.
        ttc = price + (0.80 if price < 10 else price * 0.05 + 0.70)
        price_str = f"{price:.2f} EUR  |  ≈ {ttc:.2f} EUR TTC (estimation)"
    else:
        price_str = "—"

    user = item.get("user") or {}
    item_url = _safe_url(item.get("url"), f"{VINTED_BASE}/items/{item.get('id','')}")
    # Confirmé : le nouvel endpoint ne renvoie jamais "profile_url" dans
    # l'objet user (seulement id/login/business) — on le reconstruit.
    author_url = _safe_url(
        user.get("profile_url"),
        f"{VINTED_BASE}/member/{user['id']}" if user.get("id") else VINTED_BASE,
    )

    embed = {
        "author": {
            "name": (user.get("login") or "—")[:256] or "—",
            "url":  author_url,
        },
        "title": (item.get("title") or "Sans titre")[:256] or "Sans titre",
        "url":   item_url,
        "color": 0x09B1BA,
        "fields": [
            {"name": "⏱️ Publié", "value": get_published(item) or "—", "inline": True},
            {"name": "🏷️ Marque", "value": (get_brand(item) or "—")[:50] or "—", "inline": True},
            {"name": "📐 Taille", "value": (get_size(item) or "—")[:30] or "—",  "inline": True},
            {"name": "💎 État",   "value": get_condition(item) or "—",  "inline": True},
            {"name": "💰 Prix",   "value": price_str,            "inline": False},
        ],
        "footer": {"text": f"Vinted Bot · {search_name}"[:2048]},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    img = get_image(item)
    if img and isinstance(img, str) and img.startswith(("http://", "https://")):
        embed["image"] = {"url": img}
    return embed

def build_buttons(item: dict) -> list:
    # C'était le bug : item.get("url") est relatif ("/items/123-...") sur le
    # nouvel endpoint, jamais passé par _safe_url ici contrairement à
    # l'embed — Discord rejette (400) une URL de bouton non absolue.
    url = _safe_url(item.get("url"), f"{VINTED_BASE}/items/{item.get('id','')}")
    return [{
        "type": 1,
        "components": [
            {"type": 2, "style": 5, "label": "🔍 Détails",  "url": url},
            {"type": 2, "style": 5, "label": "🛒 Acheter",  "url": url},
            {"type": 2, "style": 5, "label": "🤝 Négocier", "url": url},
        ],
    }]

async def discord_sender(webhook_url: str):
    """
    Consomme la file d'envoi indépendamment du scan.
    Le scanner n'est jamais bloqué par le rate-limit Discord.
    """
    async with AsyncSession(timeout=15) as sess:
        while True:
            item, search_name = await send_queue.get()
            payload = {
                "username": "Vinted Bot 🛍️",
                "embeds": [build_embed(item, search_name)],
                "components": build_buttons(item),
            }
            try:
                r = await sess.post(webhook_url, json=payload)
                if r.status_code == 429:
                    wait = 5.0
                    try:
                        wait = float(r.json().get("retry_after", 5))
                    except Exception:
                        pass
                    state.log(f"Rate-limit Discord — attente {wait:.1f}s", "WARN")
                    await asyncio.sleep(wait)
                    r = await sess.post(webhook_url, json=payload)

                if 200 <= r.status_code < 300:
                    state.stats["sent"] += 1
                else:
                    state.stats["errors"] += 1
                    try:
                        detail = r.text[:300]
                    except Exception:
                        detail = "(corps illisible)"
                    state.log(f"Discord a refusé l'envoi (statut {r.status_code}) : {detail}", "ERROR")
            except Exception as e:
                state.stats["errors"] += 1
                state.log(f"Erreur envoi Discord : {type(e).__name__}", "ERROR")
            finally:
                send_queue.task_done()
                state.touch()
                await asyncio.sleep(DISCORD_SEND_DELAY)


# ═══════════════════════════════════════════════════════════════════
#  PLANIFICATEUR
# ═══════════════════════════════════════════════════════════════════
class Scheduler:
    """
    Chaque recherche a sa propre échéance et son propre intervalle.
    Les recherches dues sont exécutées en parallèle (limitées par un
    sémaphore). Un réveil immédiat est possible depuis Discord.
    """
    def __init__(self, config: dict):
        self.config   = config
        self.sem      = asyncio.Semaphore(MAX_CONCURRENT_SEARCHES)
        self.next_run: dict[str, float] = {}
        self.wakeup   = asyncio.Event()
        self.resume   = asyncio.Event()
        self.resume.set()                  # non-pausé par défaut
        self.primed: set[str] = set()      # recherches déjà initialisées

    @property
    def default_interval(self) -> int:
        return int(self.config.get("poll_interval_seconds", 60))

    def interval_for(self, cfg: dict) -> float:
        base = max(MIN_INTERVAL, float(cfg.get("interval") or self.default_interval))
        jittered = base * (1 + random.uniform(-JITTER_RATIO, JITTER_RATIO))
        return max(MIN_INTERVAL, jittered)   # le plancher prime sur le jitter

    def active_searches(self) -> list[dict]:
        return [s for s in self.config.get("searches", []) if s.get("enabled", True)]

    def sync_state(self):
        state.searches = [
            {
                "name":      s.get("name", ""),
                "keywords":  s.get("keywords", ""),
                "url":       s.get("url", ""),
                "brands":    s.get("brands", []),
                "price_min": s.get("price_min"),
                "price_max": s.get("price_max"),
                "interval":  s.get("interval") or self.default_interval,
                "enabled":   s.get("enabled", True),
                "seen":      seen.count(s.get("name", "")),
            }
            for s in self.config.get("searches", [])
        ]
        state.touch()

    async def scan(self, cfg: dict):
        name = cfg.get("name", "recherche")
        async with self.sem:
            items = await vinted.search(cfg)
            if not items:
                return

            # Amorçage uniquement si la recherche n'a AUCUN historique persisté.
            # Après un redémarrage, l'historique sur disque suffit à éviter le
            # flood : on notifie donc bien les annonces parues pendant l'arrêt.
            first_run = name not in self.primed and seen.count(name) == 0

            candidates = []
            for item in items:
                iid = str(item.get("id", ""))
                if not iid or seen.has(name, iid):
                    continue
                seen.add(name, iid)
                if passes_filters(item, cfg):
                    candidates.append(item)

            self.primed.add(name)

            if first_run:
                state.log(f"« {name} » amorcée — {len(items)} annonce(s) mémorisée(s), aucune notification", "SUCCESS")
                return

            if not candidates:
                state.log(f"« {name} » — rien de nouveau ({len(items)} scannées)", "INFO")
                return

            # Garde-fou : jamais de rafale, même après un long arrêt
            if len(candidates) > MAX_NOTIFY_PER_SCAN:
                state.log(
                    f"« {name} » — {len(candidates)} nouvelles annonces, "
                    f"seules les {MAX_NOTIFY_PER_SCAN} plus récentes sont notifiées",
                    "WARN",
                )
                candidates = candidates[:MAX_NOTIFY_PER_SCAN]

            for item in candidates:
                state.stats["found"] += 1
                state.add_item(item, name)
                state.log(f"★ {item.get('title','?')[:55]} — {get_price(item):.2f}€  [{name}]", "FOUND")
                await send_queue.put((item, name))

    async def run(self):
        state.status = "running"
        state.started_at = datetime.now().strftime("%H:%M:%S")
        self.sync_state()

        while True:
            await self.resume.wait()      # bloque instantanément si en pause

            now = time.monotonic()
            searches = self.active_searches()

            if not searches:
                state.log("Aucune recherche active — en attente", "WARN")
                await self._sleep_until(now + 10)
                continue

            due = []
            for cfg in searches:
                name = cfg.get("name", "")
                if self.next_run.get(name, 0) <= now:
                    due.append(cfg)

            if due:
                state.cycle += 1
                state.last_cycle = datetime.now().strftime("%H:%M:%S")
                state.proxy_status = proxy_mgr.status_line()
                names = ", ".join(c.get("name", "?") for c in due)
                state.log(f"── Cycle #{state.cycle} — {len(due)} recherche(s) : {names}", "INFO")

                # Décalage léger entre le lancement de chaque recherche du cycle.
                # Hypothèse à valider en conditions réelles : Vinted semble
                # renvoyer un corps vide (statut/Content-Type valides) quand
                # plusieurs requêtes "réelles" (hors probe) arrivent quasi
                # simultanément sur la même session. Un petit décalage réduit
                # cette simultanéité sans perdre le bénéfice du parallélisme
                # (les recherches tournent toujours en overlap, juste pas au
                # même tick exact).
                async def _staggered(i: int, c: dict):
                    if i > 0:
                        await asyncio.sleep(i * 0.4)
                    await self.scan(c)

                await asyncio.gather(
                    *(_staggered(i, c) for i, c in enumerate(due)),
                    return_exceptions=True,
                )

                for cfg in due:
                    self.next_run[cfg.get("name", "")] = time.monotonic() + self.interval_for(cfg)

                s = state.stats
                state.log(
                    f"trouvées:{s['found']} · envoyées:{s['sent']} · "
                    f"requêtes:{s['requests']} · erreurs:{s['errors']} · "
                    f"file:{send_queue.qsize()}",
                    "INFO",
                )
                seen.flush()

            # Dort jusqu'à la prochaine échéance, réveillable à tout moment
            upcoming = [
                self.next_run.get(c.get("name", ""), 0)
                for c in self.active_searches()
            ]
            target = min(upcoming) if upcoming else time.monotonic() + 5
            await self._sleep_until(target)

    async def _sleep_until(self, target_monotonic: float):
        delay = max(0.5, target_monotonic - time.monotonic())
        self.wakeup.clear()
        try:
            await asyncio.wait_for(self.wakeup.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass

    # ── Contrôle depuis Discord ──────────────────────────────────────
    def pause(self):
        self.resume.clear()
        state.paused = True
        state.status = "paused"
        state.touch()

    def unpause(self):
        self.resume.set()
        self.wakeup.set()
        state.paused = False
        state.status = "running"
        state.touch()

    def reschedule_now(self):
        """Force la prise en compte immédiate d'un changement."""
        self.wakeup.set()

    def save_config(self):
        try:
            CONFIG_FILE.write_text(
                json.dumps(self.config, indent=2, ensure_ascii=False), encoding="utf-8"
            )
        except Exception as e:
            state.log(f"Échec sauvegarde config : {e}", "ERROR")


scheduler: Scheduler | None = None


# ═══════════════════════════════════════════════════════════════════
#  CONTRÔLE DISCORD
# ═══════════════════════════════════════════════════════════════════
CMD_PREFIX = "!vinted"

def _embed(title: str, desc: str, color: int) -> discord.Embed:
    e = discord.Embed(title=title, description=desc, color=color)
    e.set_footer(text="Vinted Bot v6")
    return e

def ok(t, d):   return _embed(f"✅ {t}", d, 0x09B1BA)
def err(d):     return _embed("❌ Erreur", d, 0xF87171)
def info(t, d): return _embed(f"ℹ️ {t}", d, 0xA78BFA)


class Controller(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(intents=intents)

    async def on_ready(self):
        state.log(f"Contrôle Discord actif : {self.user}", "SUCCESS")

    async def on_message(self, msg: discord.Message):
        if msg.author.bot or not msg.content.lower().startswith(CMD_PREFIX):
            return
        parts = msg.content[len(CMD_PREFIX):].strip().split(None, 1)
        cmd   = parts[0].lower() if parts else "help"
        args  = parts[1].strip() if len(parts) > 1 else ""

        handler = getattr(self, f"cmd_{cmd}", None)
        if handler:
            try:
                await handler(msg, args)
            except Exception as e:
                await msg.channel.send(embed=err(f"`{type(e).__name__}` — {e}"))
        else:
            await msg.channel.send(embed=err(f"Commande inconnue : `{cmd}`\n`!vinted help` pour la liste."))

    # ── Commandes ────────────────────────────────────────────────────
    async def cmd_help(self, msg, _):
        await msg.channel.send(embed=info("Commandes", "\n".join([
            "`!vinted status` — état, stats, file d'envoi",
            "`!vinted searches` — liste des recherches",
            "`!vinted pause` / `resume`",
            "`!vinted interval <s>` — intervalle global",
            "`!vinted every <nom> <s>` — intervalle d'une recherche",
            "`!vinted add <nom> | url | <URL Vinted>`",
            "`!vinted add <nom> | <mots-clés> | <marques> | <min> | <max>`",
            "`!vinted enable|disable|delete <nom>`",
            "`!vinted scan` — force un scan immédiat",
            "`!vinted proxies` — état des proxies",
        ])))

    async def cmd_status(self, msg, _):
        s = state.stats
        label = {"running": "🟢 En cours", "paused": "⏸️ En pause",
                 "connecting": "🟡 Connexion", "error": "🔴 Erreur"}.get(state.status, "⚪")
        await msg.channel.send(embed=info("Statut", "\n".join([
            f"**État :** {label}",
            f"**Cycle :** #{state.cycle} · dernier à {state.last_cycle or '—'}",
            f"**Démarré :** {state.started_at or '—'}",
            "",
            f"📦 Trouvées **{s['found']}** · 📤 Envoyées **{s['sent']}**",
            f"🌐 Requêtes **{s['requests']}** · ❌ Erreurs **{s['errors']}**",
            f"📬 File d'envoi : **{send_queue.qsize()}**",
            "",
            f"🔀 {state.proxy_status}",
        ])))

    async def cmd_searches(self, msg, _):
        if not state.searches:
            await msg.channel.send(embed=err("Aucune recherche configurée."))
            return
        lines = []
        for i, s in enumerate(state.searches, 1):
            dot = "🟢" if s["enabled"] else "🔴"
            mode = f"URL `{s['url'][:50]}…`" if s.get("url") else \
                   f"kw `{s.get('keywords') or '—'}` · marques `{', '.join(s.get('brands') or []) or '—'}`"
            lines.append(f"{dot} **{i}. {s['name']}** — {s['interval']}s · {s['seen']} vues\n   {mode}")
        await msg.channel.send(embed=info(f"{len(state.searches)} recherche(s)", "\n".join(lines)))

    async def cmd_pause(self, msg, _):
        scheduler.pause()
        state.log(f"Pause demandée par {msg.author}", "WARN")
        await msg.channel.send(embed=ok("Pause", "Scan suspendu immédiatement."))

    async def cmd_resume(self, msg, _):
        scheduler.unpause()
        state.log(f"Reprise demandée par {msg.author}", "SUCCESS")
        await msg.channel.send(embed=ok("Reprise", "Scan relancé."))

    async def cmd_scan(self, msg, _):
        scheduler.next_run.clear()
        scheduler.reschedule_now()
        await msg.channel.send(embed=ok("Scan forcé", "Toutes les recherches passent maintenant."))

    async def cmd_interval(self, msg, args):
        if not args.isdigit() or int(args) < int(MIN_INTERVAL):
            await msg.channel.send(embed=err("Usage : `!vinted interval <secondes ≥ 15>`"))
            return
        scheduler.config["poll_interval_seconds"] = int(args)
        scheduler.save_config()
        scheduler.sync_state()
        scheduler.reschedule_now()
        await msg.channel.send(embed=ok("Intervalle global", f"**{args}s** — effectif immédiatement."))

    async def cmd_every(self, msg, args):
        bits = args.rsplit(None, 1)
        if len(bits) != 2 or not bits[1].isdigit() or int(bits[1]) < int(MIN_INTERVAL):
            await msg.channel.send(embed=err("Usage : `!vinted every <nom> <secondes ≥ 15>`"))
            return
        name, secs = bits[0], int(bits[1])
        for s in scheduler.config["searches"]:
            if s["name"].lower() == name.lower():
                s["interval"] = secs
                scheduler.save_config()
                scheduler.sync_state()
                scheduler.reschedule_now()
                await msg.channel.send(embed=ok("Intervalle mis à jour", f"**{s['name']}** → {secs}s"))
                return
        await msg.channel.send(embed=err(f"Recherche introuvable : `{name}`"))

    async def cmd_add(self, msg, args):
        parts = [p.strip() for p in args.split("|")]
        if len(parts) < 2:
            await msg.channel.send(embed=err(
                "`!vinted add <nom> | url | <URL Vinted>`\n"
                "`!vinted add <nom> | <mots-clés> | <marques> | <min> | <max>`"
            ))
            return

        name = parts[0] or "Nouvelle recherche"
        if parts[1].lower() == "url" and len(parts) >= 3:
            new = {"name": name, "url": parts[2], "enabled": True}
            desc = f"**{name}**\nURL : `{parts[2][:70]}…`"
        elif parts[1].startswith("http"):
            new = {"name": name, "url": parts[1], "enabled": True}
            desc = f"**{name}**\nURL : `{parts[1][:70]}…`"
        else:
            def _int(i):
                try:
                    return int(parts[i]) if len(parts) > i and parts[i] else None
                except ValueError:
                    return None
            brands = [b.strip() for b in parts[2].split(",")] if len(parts) > 2 and parts[2] else []
            new = {"name": name, "keywords": parts[1], "brands": brands,
                   "price_min": _int(3), "price_max": _int(4), "enabled": True}
            desc = (f"**{name}**\nMots-clés `{parts[1]}` · marques `{', '.join(brands) or '—'}`\n"
                    f"Prix `{new['price_min'] or '—'}–{new['price_max'] or '—'}€`")

        scheduler.config["searches"].append(new)
        scheduler.save_config()
        scheduler.sync_state()
        scheduler.reschedule_now()
        state.log(f"Recherche ajoutée : « {name} » par {msg.author}", "SUCCESS")
        await msg.channel.send(embed=ok("Recherche ajoutée", desc + "\n\nAmorçage au prochain passage, puis notifications."))

    async def _toggle(self, msg, name, value):
        for s in scheduler.config["searches"]:
            if s["name"].lower() == name.lower():
                s["enabled"] = value
                scheduler.save_config()
                scheduler.sync_state()
                scheduler.reschedule_now()
                verb = "activée" if value else "désactivée"
                await msg.channel.send(embed=ok(f"Recherche {verb}", f"**{s['name']}**"))
                return
        await msg.channel.send(embed=err(f"Recherche introuvable : `{name}`"))

    async def cmd_enable(self, msg, args):
        await self._toggle(msg, args, True)

    async def cmd_disable(self, msg, args):
        await self._toggle(msg, args, False)

    async def cmd_delete(self, msg, args):
        before = len(scheduler.config["searches"])
        scheduler.config["searches"] = [
            s for s in scheduler.config["searches"] if s["name"].lower() != args.lower()
        ]
        if len(scheduler.config["searches"]) < before:
            scheduler.save_config()
            scheduler.sync_state()
            state.log(f"Recherche supprimée : « {args} »", "WARN")
            await msg.channel.send(embed=ok("Supprimée", f"**{args}**"))
        else:
            await msg.channel.send(embed=err(f"Recherche introuvable : `{args}`"))

    async def cmd_proxies(self, msg, _):
        if not proxy_mgr.enabled:
            await msg.channel.send(embed=info("Proxies", "Désactivés dans `config.json`."))
            return
        alive = proxy_mgr._alive()
        lines = [f"**{proxy_mgr.status_line()}**", ""]
        lines += [f"✅ `{ProxyManager.mask(p)}`" for p in alive]
        lines += [f"⏳ `{ProxyManager.mask(p)}`" for p in proxy_mgr.proxies if p not in alive]
        await msg.channel.send(embed=info("Proxies", "\n".join(lines)))


# ═══════════════════════════════════════════════════════════════════
#  DASHBOARD (HTTP + SSE)
# ═══════════════════════════════════════════════════════════════════
class DashHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/events":
            self._sse()
        elif path == "/state":
            self._json(json.dumps(state.snapshot(), ensure_ascii=False, default=str).encode())
        elif path in ("/", "/dashboard.html"):
            self._html()
        else:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()

    def _json(self, body: bytes):
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _html(self):
        if not DASH_FILE.exists():
            body = b"dashboard.html introuvable dans ce dossier."
            self.send_response(404)
        else:
            body = DASH_FILE.read_bytes()
            self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _sse(self):
        """Pousse l'état au navigateur dès qu'il change — pas de polling."""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        last = -1
        try:
            while True:
                if state.version != last:
                    last = state.version
                    payload = json.dumps(state.snapshot(), ensure_ascii=False, default=str)
                    self.wfile.write(f"data: {payload}\n\n".encode("utf-8"))
                    self.wfile.flush()
                else:
                    self.wfile.write(b": ping\n\n")   # garde la connexion vivante
                    self.wfile.flush()
                time.sleep(0.4)
        except Exception:
            return   # navigateur déconnecté

    def log_message(self, *args):
        pass


def start_dashboard():
    try:
        srv = ThreadingHTTPServer((DASH_HOST, DASH_PORT), DashHandler)
        srv.daemon_threads = True
        threading.Thread(target=srv.serve_forever, daemon=True, name="dashboard").start()
        log(f"Dashboard → http://localhost:{DASH_PORT}", "SUCCESS")
    except OSError as e:
        log(f"Dashboard indisponible (port {DASH_PORT}) : {e}", "WARN")


# ═══════════════════════════════════════════════════════════════════
#  TÂCHES DE FOND
# ═══════════════════════════════════════════════════════════════════
async def state_flusher():
    """Écrit bot_state.json au plus une fois par seconde."""
    while True:
        await asyncio.sleep(STATE_FLUSH_INTERVAL)
        state.flush_if_dirty()


# ═══════════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════════
def load_config() -> dict:
    if CONFIG_FILE.exists():
        cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        log(f"Config chargée ({len(cfg.get('searches', []))} recherche(s))", "SUCCESS")
    else:
        CONFIG_FILE.write_text(json.dumps(DEFAULT_CONFIG, indent=2, ensure_ascii=False), encoding="utf-8")
        log("config.json créé — édite-le puis relance", "WARN")
        cfg = DEFAULT_CONFIG

    # Sur Railway, les secrets se règlent en Variables d'environnement plutôt
    # que dans config.json (qu'on ne veut jamais committer dans un repo Git
    # avec un vrai token dedans). S'ils sont présents, ils priment.
    if os.environ.get("DISCORD_WEBHOOK_URL"):
        cfg["discord_webhook_url"] = os.environ["DISCORD_WEBHOOK_URL"]
    if os.environ.get("DISCORD_BOT_TOKEN"):
        cfg["discord_bot_token"] = os.environ["DISCORD_BOT_TOKEN"]

    return cfg


def valid_token(token: str) -> bool:
    return bool(token) and token != "COLLE_TON_BOT_TOKEN_ICI" and token.count(".") >= 2 and len(token) > 50


# ═══════════════════════════════════════════════════════════════════
#  POINT D'ENTRÉE
# ═══════════════════════════════════════════════════════════════════
async def main():
    global scheduler, proxy_mgr, send_queue

    send_queue = asyncio.Queue()

    print()
    print(f"{Style.BRIGHT}{Fore.CYAN}{'═' * 54}")
    print(f"   VINTED BOT {BOT_VERSION}  —  asynchrone  —  impersonate={IMPERSONATE}")
    print(f"{'═' * 54}{Style.RESET_ALL}")
    print()

    config = load_config()
    webhook = config.get("discord_webhook_url", "")
    if not webhook or webhook == "COLLE_TON_WEBHOOK_ICI":
        log("Webhook Discord non configuré dans config.json", "ERROR")
        sys.exit(1)

    proxy_mgr = ProxyManager(config.get("proxies", {}))
    state.proxy_status = proxy_mgr.status_line()

    scheduler = Scheduler(config)
    start_dashboard()

    await vinted.open()
    state.status = "connecting"
    state.log("Connexion à Vinted…", "INFO")

    connected = False
    for attempt in range(1, 4):
        if await vinted.ensure_session(force=True):
            connected = True
            break
        state.log(f"Tentative {attempt}/3 échouée — nouvel essai dans 5s", "WARN")
        await asyncio.sleep(5)

    if not connected:
        state.status = "error"
        state.log("Impossible d'établir une session Vinted.", "ERROR")
        await vinted.close()
        sys.exit(1)

    tasks = [
        asyncio.create_task(scheduler.run(),          name="scheduler"),
        asyncio.create_task(discord_sender(webhook),  name="sender"),
        asyncio.create_task(state_flusher(),          name="flusher"),
    ]

    token = config.get("discord_bot_token", "")
    controller = None
    if valid_token(token):
        controller = Controller()
        tasks.append(asyncio.create_task(controller.start(token), name="discord"))
    else:
        log("Token Discord absent ou mal formé — contrôle bidirectionnel désactivé", "WARN")

    # Railway (et tout orchestrateur de conteneurs) envoie SIGTERM à chaque
    # redéploiement ou arrêt — pas SIGINT (Ctrl+C), qui ne concerne que
    # l'usage local au clavier. Sans ce handler, SIGTERM tue le process
    # immédiatement : la sauvegarde de seen_items.json/bot_state.json
    # n'aurait pas lieu, et Railway redéploie très souvent (chaque push).
    stop_event = asyncio.Event()

    def _on_stop_signal():
        state.log("Signal d'arrêt reçu — sauvegarde en cours…", "WARN")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _on_stop_signal)
        except NotImplementedError:
            pass  # Windows : SIGINT reste géré via KeyboardInterrupt plus bas

    try:
        stop_waiter = asyncio.create_task(stop_event.wait(), name="stop-waiter")
        await asyncio.wait([*tasks, stop_waiter], return_when=asyncio.FIRST_COMPLETED)
    except asyncio.CancelledError:
        pass
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if controller:
            try:
                await controller.close()
            except Exception:
                pass
        await vinted.close()
        seen.flush()
        state.status = "stopped"
        state.flush_if_dirty()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print()
        log("Arrêt demandé — sauvegarde en cours…", "WARN")
        seen.flush()
        state.status = "stopped"
        state.flush_if_dirty()
        s = state.stats
        log(f"Bilan → trouvées:{s['found']} · envoyées:{s['sent']} · requêtes:{s['requests']}", "INFO")
