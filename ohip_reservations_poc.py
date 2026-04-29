"""
OHIP Reservations PoC — Oracle Hospitality Integration Platform
================================================================
Extração de reservas recentes do OPERA Cloud via API REST.

Cobre:
  1. OAuth 2.0 (Resource Owner Password OU Client Credentials)
  2. Renovação automática do token (50 min de margem antes da expiração)
  3. Chamada paginada ao endpoint getReservations
  4. Backoff exponencial em 429 / 5xx
  5. Persistência em JSON Lines (1 reserva por linha) — formato AI/BI-friendly

Pré-requisitos:
  pip install requests tenacity python-dotenv

Variáveis de ambiente esperadas (arquivo .env):
  OHIP_HOST=https://your-host.hospitality.oracleindustry.com
  OHIP_APP_KEY=41ecd082-8997-4c69-af34-2f72b83645ff
  OHIP_CLIENT_ID=...
  OHIP_CLIENT_SECRET=...
  OHIP_USERNAME=...                # apenas grant=password
  OHIP_PASSWORD=...                # apenas grant=password
  OHIP_ENTERPRISE_ID=...           # apenas grant=client_credentials
  OHIP_SCOPE=urn:opc:hgbu:ws:__myscopes__   # apenas grant=client_credentials
  OHIP_GRANT_TYPE=password         # password | client_credentials
  OHIP_HOTEL_ID=PROPCODE
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from base64 import b64encode
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

import requests
from dotenv import load_dotenv
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)


ENTERPRISE_ID_RE = re.compile(r"^[A-Z0-9]{1,8}$")


def _retry_transient(exc: BaseException) -> bool:
    """Re-tenta apenas em erros transitórios: rede, 429, 5xx. 4xx (exceto 429) falha rápido."""
    if isinstance(exc, requests.HTTPError):
        resp = getattr(exc, "response", None)
        if resp is None:
            return True
        return resp.status_code == 429 or resp.status_code >= 500
    if isinstance(exc, requests.RequestException):
        return True
    return False

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
log = logging.getLogger("ohip")


# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
@dataclass
class OhipConfig:
    host: str
    app_key: str
    client_id: str
    client_secret: str
    grant_type: str = "password"
    username: str | None = None
    password: str | None = None
    enterprise_id: str | None = None
    scope: str | None = None
    hotel_id: str | None = None

    @classmethod
    def from_env(cls) -> "OhipConfig":
        required = ["OHIP_HOST", "OHIP_APP_KEY", "OHIP_CLIENT_ID", "OHIP_CLIENT_SECRET"]
        missing = [k for k in required if not os.getenv(k)]
        if missing:
            raise RuntimeError(f"Variáveis ausentes: {missing}")
        return cls(
            host=os.environ["OHIP_HOST"].rstrip("/"),
            app_key=os.environ["OHIP_APP_KEY"],
            client_id=os.environ["OHIP_CLIENT_ID"],
            client_secret=os.environ["OHIP_CLIENT_SECRET"],
            grant_type=os.getenv("OHIP_GRANT_TYPE", "password"),
            username=os.getenv("OHIP_USERNAME"),
            password=os.getenv("OHIP_PASSWORD"),
            enterprise_id=os.getenv("OHIP_ENTERPRISE_ID"),
            scope=os.getenv("OHIP_SCOPE"),
            hotel_id=os.getenv("OHIP_HOTEL_ID"),
        )


# ---------------------------------------------------------------------------
# Cliente OAuth com auto-renovação
# ---------------------------------------------------------------------------
@dataclass
class TokenCache:
    access_token: str = ""
    expires_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def is_valid(self) -> bool:
        # margem de segurança de 10 min (Oracle recomenda renovar a cada 50 min;
        # tokens vivem 60 min, conforme a doc oficial).
        return bool(self.access_token) and datetime.now(timezone.utc) < (
            self.expires_at - timedelta(minutes=10)
        )


class OhipClient:
    """Cliente HTTP com OAuth, retry e paginação para OPERA Cloud."""

    def __init__(self, cfg: OhipConfig) -> None:
        self.cfg = cfg
        self._token = TokenCache()
        self._session = requests.Session()

    # --- Autenticação -----------------------------------------------------
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=20),
        retry=retry_if_exception(_retry_transient),
        reraise=True,
    )
    def _fetch_token(self) -> None:
        url = f"{self.cfg.host}/oauth/v1/tokens"
        basic = b64encode(
            f"{self.cfg.client_id}:{self.cfg.client_secret}".encode()
        ).decode()
        headers = {
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
            "x-app-key": self.cfg.app_key,
            "X-Request-Id": str(uuid4()),
        }
        if self.cfg.grant_type == "client_credentials":
            if not self.cfg.scope:
                raise RuntimeError("client_credentials exige OHIP_SCOPE.")
            # enterpriseId é exigido apenas em ambientes OCIM. Quando definido,
            # precisa casar o pattern da spec OHIP: ^[A-Z0-9]{1,8}$.
            if self.cfg.enterprise_id:
                if not ENTERPRISE_ID_RE.match(self.cfg.enterprise_id):
                    raise RuntimeError(
                        f"OHIP_ENTERPRISE_ID inválido ({self.cfg.enterprise_id!r}): "
                        "deve casar ^[A-Z0-9]{1,8}$ (maiúsculas e dígitos, até 8 chars)."
                    )
                headers["enterpriseId"] = self.cfg.enterprise_id
            body = {"grant_type": "client_credentials", "scope": self.cfg.scope}
        else:  # password (Resource Owner)
            if not (self.cfg.username and self.cfg.password):
                raise RuntimeError("grant=password exige OHIP_USERNAME e OHIP_PASSWORD.")
            body = {
                "grant_type": "password",
                "username": self.cfg.username,
                "password": self.cfg.password,
            }

        log.info("Solicitando token OAuth (grant=%s)…", self.cfg.grant_type)
        r = self._session.post(url, headers=headers, data=body, timeout=30)
        if r.status_code >= 400:
            log.error(
                "Falha no token endpoint: %s %s — body: %s",
                r.status_code, r.reason, r.text[:500],
            )
        r.raise_for_status()
        data = r.json()
        self._token = TokenCache(
            access_token=data["access_token"],
            expires_at=datetime.now(timezone.utc)
            + timedelta(seconds=int(data.get("expires_in", 3600))),
        )
        log.info("Token obtido. Expira em %s.", self._token.expires_at.isoformat())

    def _ensure_token(self) -> str:
        if not self._token.is_valid:
            self._fetch_token()
        return self._token.access_token

    # --- HTTP genérico com rate-limit awareness ---------------------------
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        retry=retry_if_exception(_retry_transient),
        reraise=True,
    )
    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        token = self._ensure_token()
        headers = kwargs.pop("headers", {}) or {}
        headers.update(
            {
                "Authorization": f"Bearer {token}",
                "x-app-key": self.cfg.app_key,
                "X-Request-Id": str(uuid4()),
                "Accept": "application/json",
            }
        )
        if self.cfg.hotel_id:
            headers.setdefault("x-hotelid", self.cfg.hotel_id)

        url = f"{self.cfg.host}{path}"
        r = self._session.request(method, url, headers=headers, timeout=60, **kwargs)

        # 401 → token revogado/forçar renovação e re-tentar uma vez
        if r.status_code == 401:
            log.warning("401 Unauthorized — forçando renovação do token.")
            self._token = TokenCache()
            raise requests.HTTPError("401, token renovação", response=r)

        # 429 → respeitar Retry-After
        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", "5"))
            log.warning("429 rate limit — aguardando %ss.", retry_after)
            time.sleep(retry_after)
            raise requests.HTTPError("429 rate limited", response=r)

        if r.status_code >= 500:
            log.error("5xx em %s %s — body: %s", method, path, r.text[:500])
            raise requests.HTTPError(f"{r.status_code} server error", response=r)

        if r.status_code >= 400:
            log.error(
                "Erro %s em %s %s — body: %s",
                r.status_code, method, path, r.text[:500],
            )
        r.raise_for_status()
        return r

    # --- getReservations (paginado) ---------------------------------------
    def fetch_reservations(
        self,
        arrival_start: str,
        arrival_end: str,
        page_size: int = 100,
        fetch_instructions: list[str] | None = None,
    ) -> Iterator[dict]:
        """
        Itera sobre TODAS as reservas no intervalo, lidando com paginação.

        OPERA Cloud usa offset/limit no endpoint /rsv/v1/hotels/{hotelId}/reservations.
        Quando a resposta retorna menos itens que `limit`, encerramos.

        Args:
            arrival_start: 'YYYY-MM-DD'
            arrival_end:   'YYYY-MM-DD'
            page_size:     itens por página (a doc do OHIP costuma limitar a 100–200)
            fetch_instructions: blocos de detalhe (ex.: ['Comments','Profile','Reservation'])
        """
        if not self.cfg.hotel_id:
            raise RuntimeError("OHIP_HOTEL_ID é obrigatório para getReservations.")

        path = f"/rsv/v1/hotels/{self.cfg.hotel_id}/reservations"
        offset = 0
        page = 0
        while True:
            params: dict[str, Any] = {
                "limit": page_size,
                "offset": offset,
                "arrivalStartDate": arrival_start,
                "arrivalEndDate": arrival_end,
            }
            if fetch_instructions:
                params["fetchInstructions"] = fetch_instructions

            log.info("Página %d (offset=%d)…", page, offset)
            r = self._request("GET", path, params=params)
            payload = r.json()

            # O envelope típico do OHIP é { "reservations": { "reservationInfo": [...] } }
            # Caímos em um caminho defensivo se o envelope variar entre tenants.
            items = (
                payload.get("reservations", {}).get("reservationInfo")
                or payload.get("hotelReservations", {}).get("reservationInfo")
                or payload.get("reservationInfo")
                or []
            )
            if not items:
                log.info("Sem mais resultados. Total páginas=%d.", page)
                break

            for item in items:
                yield item

            if len(items) < page_size:
                log.info("Última página detectada (parcial). Páginas=%d.", page + 1)
                break

            offset += page_size
            page += 1
            # rate-limit cooperativo: pequena pausa entre páginas
            time.sleep(0.25)


# ---------------------------------------------------------------------------
# Persistência: JSON Lines (Bronze layer — pronto p/ DuckDB / BigQuery / Snowflake)
# ---------------------------------------------------------------------------
def write_jsonl(records: Iterator[dict], out_dir: Path, hotel_id: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = out_dir / f"reservations_{hotel_id}_{ts}.jsonl"
    n = 0
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            # Anotação de metadados de ingestão (lineage para o lakehouse)
            rec["_ingested_at"] = datetime.now(timezone.utc).isoformat()
            rec["_source"] = "ohip.opera_cloud.rsv.getReservations"
            rec["_hotel_id"] = hotel_id
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            n += 1
    log.info("Gravado %d registros em %s", n, path)
    return path


# ---------------------------------------------------------------------------
# Entry point — PoC
# ---------------------------------------------------------------------------
def main() -> None:
    cfg = OhipConfig.from_env()
    client = OhipClient(cfg)

    # Janela: últimos 7 dias até hoje
    today = datetime.now(timezone.utc).date()
    arrival_start = (today - timedelta(days=7)).isoformat()
    arrival_end = today.isoformat()

    # OHIP_HOTEL_ID pode ser um código ou uma lista (vírgula): "CUMBUCO,TAIBA,CHARME,MAGNA"
    hotel_ids = [h.strip() for h in (cfg.hotel_id or "").split(",") if h.strip()]
    if not hotel_ids:
        raise RuntimeError("OHIP_HOTEL_ID é obrigatório.")

    out_dir = Path("./data/bronze/reservations")
    for hid in hotel_ids:
        # Cada property usa seu próprio header x-hotelid; reaproveitamos o token cacheado.
        client.cfg.hotel_id = hid
        log.info("=== Property %s | %s → %s ===", hid, arrival_start, arrival_end)
        try:
            reservations = client.fetch_reservations(
                arrival_start=arrival_start,
                arrival_end=arrival_end,
                page_size=100,
                fetch_instructions=["Reservation", "Indicators", "ReservationPreferences"],
            )
            write_jsonl(reservations, out_dir, hid)
        except Exception as e:
            log.error("Falha ao processar %s: %s", hid, e)


if __name__ == "__main__":
    main()
