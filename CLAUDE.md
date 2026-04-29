# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

PoC em Python para ingestão de reservas do **OPERA Cloud** via **OHIP** (Oracle Hospitality Integration Platform). Saída em JSON Lines na camada *bronze* de um lakehouse (`./data/bronze/reservations/`).

Arquivo principal: [ohip_reservations_poc.py](ohip_reservations_poc.py) — script único, sem pacote.

## Commands

```bash
# Ambiente (venv já existe na raiz)
source venv/Scripts/activate          # Git Bash no Windows
pip install requests tenacity python-dotenv

# Executar a PoC (lê .env automaticamente)
python ohip_reservations_poc.py
```

Não há suíte de testes, lint ou build configurados.

## Architecture

Pipeline linear num único módulo:

1. **`OhipConfig.from_env()`** — carrega credenciais do `.env`. Suporta dois `OHIP_GRANT_TYPE`:
   - `password` (Resource Owner) — exige `OHIP_USERNAME`/`OHIP_PASSWORD`.
   - `client_credentials` (OCIM) — exige `OHIP_SCOPE`; `OHIP_ENTERPRISE_ID` opcional, mas quando presente **deve** casar `^[A-Z0-9]{1,8}$` (validado por `ENTERPRISE_ID_RE`).

2. **`OhipClient`** — cliente HTTP com:
   - **OAuth com cache** (`TokenCache`): renova com 10 min de margem antes da expiração informada por `expires_in`. Token endpoint: `POST {host}/oauth/v1/tokens` com `Basic` auth + header `x-app-key`.
   - **Retry via `tenacity`**: `_retry_transient` re-tenta apenas em erros de rede, 429 e 5xx; 4xx (≠429) falha rápido. 401 invalida o cache e força nova tentativa.
   - **429** respeita `Retry-After` antes de re-lançar para o decorator de retry.
   - Header `x-hotelid` é setado por requisição a partir de `cfg.hotel_id` — o loop de `main()` muta esse campo entre propriedades reaproveitando o mesmo token.

3. **`fetch_reservations()`** — paginação offset/limit em `/rsv/v1/hotels/{hotelId}/reservations`. Encerra quando a página vem parcial ou vazia. Aceita `fetchInstructions` (ex.: `Reservation`, `Indicators`, `ReservationPreferences`).
   - **Envelope defensivo**: tenta `reservations.reservationInfo`, depois `hotelReservations.reservationInfo`, depois `reservationInfo` na raiz — varia entre tenants.

4. **`write_jsonl()`** — uma reserva por linha; injeta `_ingested_at`, `_source`, `_hotel_id` para *lineage*. Nome do arquivo: `reservations_{hotelId}_{ISO8601Z}.jsonl`.

5. **`main()`** — janela fixa de **arrival últimos 7 dias até hoje (UTC)**. `OHIP_HOTEL_ID` aceita lista separada por vírgula (`"CUMBUCO,TAIBA,CHARME,MAGNA"`) — uma saída JSONL por propriedade; falha em uma property não interrompe as demais.

## API references

Specs OpenAPI/notas em [specs/](specs/):
- `OPERA Cloud Reservation API.json` — endpoints `/rsv/v1/...`
- `oAuth API for OHIP.json` — fluxo do token endpoint
- `oracle-hospitality-api-docs-8a5edab282632443.txt`

Consulte estes arquivos antes de adicionar novos endpoints; o envelope de resposta e os `fetchInstructions` válidos vêm de lá.

## Conventions

- Logs em pt-BR via `logging` (`log = logging.getLogger("ohip")`).
- `X-Request-Id` (UUID4) é gerado por requisição — útil para suporte da Oracle.
- Datas sempre em UTC (`datetime.now(timezone.utc)`).
- Nunca commitar `.env` (já no `.gitignore`).
