#!/usr/bin/env python3
"""
Play55 API Data Importer
Fetches sales data from Play55 API and stores in MySQL table play55_vendas
Replicates the functionality of play55-api.js

Supports two modes:
1. Standalone: Uses .env file for configuration
2. Airflow: Accepts connection parameters from Airflow hooks/operators
"""

import hashlib
import time
import requests
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Set, Any


# ====== Hash/Dedup Functions (matching JS logic) ======

def sha256_hex(text: str) -> str:
    """Compute SHA-256 hash of string (matches JS sha256Hex_)"""
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def stable_stringify(obj: Any) -> str:
    """Create stable string representation (matches JS stableStringify_)"""
    if obj is None:
        return "null"
    if not isinstance(obj, (dict, list)):
        return str(obj)
    if isinstance(obj, list):
        return "[" + ",".join(stable_stringify(x) for x in obj) + "]"
    # Sort keys for stability
    keys = sorted(obj.keys())
    parts = [f"{k}:{stable_stringify(obj[k])}" for k in keys]
    return "{" + ",".join(parts) + "}"


def compute_stable_key(obj: Dict) -> str:
    """
    Compute stable key for deduplication (matches JS computeStableKey_)
    Uses preferred ID fields + operador_id + sorteio_id
    """
    preferred = [
        "id", "venda_id", "id_venda", "pedido_id", "contrato_id",
        "cupom_id", "numero_bilhete", "referencia", "nsu",
        "autorizacao", "transaction_id"
    ]
    parts = []

    # Try to use a preferred unique ID (only first match)
    for k in preferred:
        if k in obj and obj[k] not in ("", None):
            parts.append(f"{k}:{obj[k]}")
            break  # Only use first match

    # Always include operador/sorteio for uniqueness per pair
    if obj.get("operador_id") not in ("", None):
        parts.append(f"op:{obj['operador_id']}")
    if obj.get("sorteio_id") not in ("", None):
        parts.append(f"so:{obj['sorteio_id']}")

    # Fallback to stable stringify if no parts
    if not parts:
        parts.append(stable_stringify(obj))

    return "|".join(parts)


# ====== Date Helpers ======

def get_date_range(
    start_date_str: Optional[str] = None,
    end_date_str: Optional[str] = None,
    always_today: bool = True,
    lookback_days: int = 0,
    use_fixed_dates: bool = False,
    fixed_start: str = "2025-12-09",
    fixed_end: str = "2025-12-09"
) -> tuple:
    """
    Resolve effective date range based on configuration
    Returns tuple (data_inicio, data_fim) in format "YYYY-MM-DDTHH:MM"

    Args:
        start_date_str: Explicit start date (YYYY-MM-DD)
        end_date_str: Explicit end date (YYYY-MM-DD)
        always_today: If True, end date is always today
        lookback_days: Number of days to look back from today (0 = today only, 1 = yesterday + today, etc.)
        use_fixed_dates: Use fixed date range (for testing)
        fixed_start: Fixed start date
        fixed_end: Fixed end date
    """
    if always_today:
        today = datetime.now()
        start_date = (today - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")
        return f"{start_date}T00:00", f"{end_date}T23:59"
    elif use_fixed_dates:
        return f"{fixed_start}T00:00", f"{fixed_end}T23:59"
    else:
        if start_date_str and end_date_str:
            return f"{start_date_str}T00:00", f"{end_date_str}T23:59"
        else:
            # Default to yesterday
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            return f"{yesterday}T00:00", f"{yesterday}T23:59"


def generate_daily_date_ranges(
    lookback_days: int = 0,
    include_today: bool = True
) -> List[tuple]:
    """
    Generate list of single-day date ranges for day-by-day API calls.

    This is useful when the API has limitations with large date ranges and
    needs to be called separately for each day.

    Args:
        lookback_days: Number of days to look back from today (0 = today only)
        include_today: Whether to include today's date in the range

    Returns:
        List of tuples [(data_inicio, data_fim), ...] for each day
        Each tuple contains ("YYYY-MM-DDTHH:MM", "YYYY-MM-DDTHH:MM")

    Example:
        If today is 2026-01-28 and lookback_days=2, include_today=True:
        Returns [
            ("2026-01-26T00:00", "2026-01-26T23:59"),
            ("2026-01-27T00:00", "2026-01-27T23:59"),
            ("2026-01-28T00:00", "2026-01-28T23:59"),
        ]
    """
    today = datetime.now()
    date_ranges = []

    # Calculate end offset: -1 to include today, 0 to exclude today
    end_offset = -1 if include_today else 0

    # Iterate from lookback_days ago to today (or yesterday)
    for i in range(lookback_days, end_offset, -1):
        date = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        date_ranges.append((f"{date}T00:00", f"{date}T23:59"))

    return date_ranges


# ====== API Functions ======

class Play55ApiClient:
    """Client for Play55 API with configurable base URL and auth token"""

    def __init__(self, base_url: str, auth_token: str):
        self.base_url = base_url.rstrip('/')
        self.auth_token = auth_token
        self.headers = {"Authorization": f"Basic {auth_token}"}

    def make_request(self, url: str) -> Dict:
        """Make HTTP request with error handling"""
        try:
            print(f"  API Request: {url}")
            response = requests.get(url, headers=self.headers, timeout=30)
            print(f"  API Response: HTTP {response.status_code}")
            return {
                "ok": 200 <= response.status_code < 300,
                "code": response.status_code,
                "text": response.text,
                "json": response.json() if response.ok else None
            }
        except Exception as e:
            print(f"  API Request Failed: {url}")
            print(f"  Error: {str(e)}")
            return {"ok": False, "code": -1, "text": str(e), "json": None}

    def get_active_pairs(self) -> List[Dict]:
        """
        Fetch all active pairs from sorteioPassivo2 endpoint
        Returns list of dicts with sorteio_id and operador_id_candidates
        """
        base_url = f"{self.base_url}/parceiros/etapas/sorteioPassivo2"
        page = 1
        pairs = []

        while True:
            url = f"{base_url}?pagina={page}"
            resp = self.make_request(url)

            if not resp["ok"]:
                print(f"Failed to fetch sorteioPassivo2 page {page}. HTTP={resp['code']}")
                break

            data = resp.get("json")
            if data is None:
                print(f"Invalid JSON in sorteioPassivo2 page {page}")
                break

            # Handle array or object with data/result key
            items = data if isinstance(data, list) else (data.get("data") or data.get("result") or [])

            if not items:
                if page == 1:
                    print("sorteioPassivo2 returned no items.")
                break

            # Extract pairs (operacao_id, operacao_codigo -> operador_id candidates)
            for item in items:
                sorteio_id = item.get("sorteio_id") or item.get("sorteioId") or item.get("id_sorteio") or item.get("etapa_id")

                candidates = []
                if item.get("operacao_id") is not None:
                    candidates.append(str(item["operacao_id"]))
                if item.get("operacao_codigo") is not None:
                    candidates.append(str(item["operacao_codigo"]))

                # Deduplicate candidates
                candidates = list(dict.fromkeys(c for c in candidates if c))

                if sorteio_id and candidates:
                    pairs.append({
                        "sorteio_id": str(sorteio_id),
                        "operador_id_candidates": candidates
                    })

            # Check for pagination end (less than 50 items)
            if len(items) < 50:
                break

            page += 1
            time.sleep(0.12)  # Small delay between requests

        # Deduplicate pairs
        seen = set()
        unique_pairs = []
        for p in pairs:
            key = f"{p['sorteio_id']}___{','.join(p['operador_id_candidates'])}"
            if key not in seen:
                seen.add(key)
                unique_pairs.append(p)

        print(f"Found {len(unique_pairs)} unique pairs from sorteioPassivo2")
        return unique_pairs

    def fetch_sales_for_pair(self, op_id: str, so_id: str, data_inicio: str, data_fim: str) -> List[Dict]:
        """
        Fetch all pages of sales for a given operador_id/sorteio_id pair
        Returns list of sale records
        """
        base_url = f"{self.base_url}/parceiros/vendasAp"
        page = 1
        out = []
        first_response_ok = False

        while True:
            url = f"{base_url}?pagina={page}&operador_id={op_id}&sorteio_id={so_id}&data_inicio={data_inicio}&data_fim={data_fim}"
            resp = self.make_request(url)

            if not resp["ok"]:
                if not first_response_ok:
                    return []
                print(f"Failed at page {page} (op={op_id}, so={so_id}). HTTP={resp['code']}")
                break

            data = resp.get("json")
            if data is None:
                if not first_response_ok:
                    return []
                print(f"Error parsing JSON at page {page} (op={op_id}, so={so_id})")
                break

            # Handle array or object with data/result key
            items = data if isinstance(data, list) else (data.get("data") or data.get("result") or [])

            if not items:
                if not first_response_ok:
                    return []
                break

            first_response_ok = True

            # Add operador_id and sorteio_id to each record if missing
            for item in items:
                if item.get("operador_id") is None:
                    item["operador_id"] = op_id
                if item.get("sorteio_id") is None:
                    item["sorteio_id"] = so_id

            out.extend(items)
            page += 1
            time.sleep(0.1)  # Small delay between requests

        return out


# ====== MySQL Functions ======

TABLE_NAME = "play55_vendas"

# Snowflake control table location
SNOWFLAKE_PAIRS_CONTROL_TABLE = "STG.APOSTA_PREMIA.PLAY55_PAIRS_CONTROL"


# ====== Control Table Functions (Snowflake) ======

def ensure_pairs_control_table(snowflake_cursor) -> None:
    """
    Create the pairs control table in Snowflake if it doesn't exist.
    This table stores which (operador_id, sorteio_id) pairs were active on each date.
    """
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_PAIRS_CONTROL_TABLE} (
        id INTEGER AUTOINCREMENT,
        capture_date DATE NOT NULL,
        sorteio_id VARCHAR(50) NOT NULL,
        operador_id VARCHAR(50) NOT NULL,
        operacao_codigo VARCHAR(100),
        created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (id),
        UNIQUE (capture_date, sorteio_id, operador_id)
    )
    """
    snowflake_cursor.execute(create_sql)
    print(f"Ensured {SNOWFLAKE_PAIRS_CONTROL_TABLE} table exists in Snowflake.")


def store_pairs_to_control_table(snowflake_cursor, capture_date: str, pairs: List[Dict]) -> int:
    """
    Store pairs for a specific date into the Snowflake control table.
    Uses INSERT with WHERE NOT EXISTS to skip duplicates (Snowflake doesn't have INSERT IGNORE).

    Args:
        snowflake_cursor: Snowflake cursor
        capture_date: Date in YYYY-MM-DD format
        pairs: List of pair dicts with sorteio_id, operador_id_candidates

    Returns:
        Number of pairs stored
    """
    if not pairs:
        return 0

    # Use INSERT ... SELECT ... WHERE NOT EXISTS for idempotent inserts
    insert_sql = f"""
    INSERT INTO {SNOWFLAKE_PAIRS_CONTROL_TABLE}
    (capture_date, sorteio_id, operador_id, operacao_codigo)
    SELECT %s, %s, %s, %s
    WHERE NOT EXISTS (
        SELECT 1 FROM {SNOWFLAKE_PAIRS_CONTROL_TABLE}
        WHERE capture_date = %s AND sorteio_id = %s AND operador_id = %s
    )
    """

    stored = 0
    for pair in pairs:
        sorteio_id = pair.get("sorteio_id")
        candidates = pair.get("operador_id_candidates", [])
        operacao_codigo = pair.get("operacao_codigo")

        # Store each operador_id candidate as a separate row
        for operador_id in candidates:
            snowflake_cursor.execute(
                insert_sql,
                (capture_date, sorteio_id, operador_id, operacao_codigo,
                 capture_date, sorteio_id, operador_id)
            )
            stored += snowflake_cursor.rowcount

    return stored


def get_pairs_from_control_table(snowflake_cursor, target_date: str) -> List[Dict]:
    """
    Retrieve pairs for a specific date from the Snowflake control table.

    Args:
        snowflake_cursor: Snowflake cursor
        target_date: Date in YYYY-MM-DD format

    Returns:
        List of pair dicts with sorteio_id and operador_id_candidates
    """
    select_sql = f"""
    SELECT sorteio_id, operador_id, operacao_codigo
    FROM {SNOWFLAKE_PAIRS_CONTROL_TABLE}
    WHERE capture_date = %s
    ORDER BY sorteio_id, operador_id
    """
    snowflake_cursor.execute(select_sql, (target_date,))
    rows = snowflake_cursor.fetchall()

    if not rows:
        return []

    # Group by sorteio_id
    pairs_dict: Dict[str, Dict] = {}
    for row in rows:
        sorteio_id, operador_id, operacao_codigo = row
        if sorteio_id not in pairs_dict:
            pairs_dict[sorteio_id] = {
                "sorteio_id": str(sorteio_id),
                "operador_id_candidates": [],
                "operacao_codigo": operacao_codigo
            }
        pairs_dict[sorteio_id]["operador_id_candidates"].append(str(operador_id))

    return list(pairs_dict.values())


def get_historical_pairs_from_snowflake(snowflake_cursor) -> List[Dict]:
    """
    Get all distinct (operador_id, sorteio_id) pairs from existing Snowflake sales data.
    This is used as a fallback when control table has no data for a specific date.

    Queries: STG.APOSTA_PREMIA.PLAY55_VENDAS (Airbyte destination table)

    Returns:
        List of pair dicts with sorteio_id and operador_id_candidates
    """
    # Query the Snowflake staging table (Airbyte destination)
    select_sql = """
    SELECT DISTINCT sorteio_id, operador_id
    FROM STG.APOSTA_PREMIA.PLAY55_VENDAS
    WHERE sorteio_id IS NOT NULL AND operador_id IS NOT NULL
    ORDER BY sorteio_id, operador_id
    """
    snowflake_cursor.execute(select_sql)
    rows = snowflake_cursor.fetchall()

    if not rows:
        return []

    # Group by sorteio_id
    pairs_dict: Dict[str, Dict] = {}
    for row in rows:
        sorteio_id, operador_id = row
        if sorteio_id not in pairs_dict:
            pairs_dict[sorteio_id] = {
                "sorteio_id": str(sorteio_id),
                "operador_id_candidates": []
            }
        if str(operador_id) not in pairs_dict[sorteio_id]["operador_id_candidates"]:
            pairs_dict[sorteio_id]["operador_id_candidates"].append(str(operador_id))

    print(f"Found {len(pairs_dict)} historical pairs from Snowflake")
    return list(pairs_dict.values())


def get_pairs_for_date(
    snowflake_cursor,
    api_client: 'Play55ApiClient',
    target_date: str,
    store_to_control: bool = True
) -> List[Dict]:
    """
    Get pairs for a specific date, using Snowflake control table if available,
    otherwise falling back to API + historical Snowflake data.

    Args:
        snowflake_cursor: Snowflake cursor for control table operations
        api_client: Play55ApiClient instance
        target_date: Date in YYYY-MM-DD format
        store_to_control: Whether to store discovered pairs to control table

    Returns:
        List of pair dicts with sorteio_id and operador_id_candidates
    """
    # First, try to get pairs from Snowflake control table
    control_pairs = get_pairs_from_control_table(snowflake_cursor, target_date)
    if control_pairs:
        print(f"  Found {len(control_pairs)} pairs in control table for {target_date}")
        return control_pairs

    # Fallback: Use ONLY API pairs (for most recent day)
    print(f"  No control data for {target_date} - using API pairs only")

    # Get current API pairs from sorteioPassivo2
    api_pairs = api_client.get_active_pairs()
    print(f"  Using {len(api_pairs)} pairs from current API call")

    # Store to Snowflake control table for future use
    if store_to_control and api_pairs:
        stored = store_pairs_to_control_table(snowflake_cursor, target_date, api_pairs)
        print(f"  Stored {stored} pair entries to control table for {target_date}")

    return api_pairs


# Column definitions for insert
INSERT_COLUMNS = [
    # Added by script
    "data",
    # From API response
    "operacao_codigo", "nsu", "sorteio_id", "sorteio_descricao",
    "comprador_nome", "comprador_email", "comprador_telefone",
    "compra_brinde", "afiliado_codigo", "data_venda",
    "valor_total", "qtd_total",
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    # Added by script
    "operador_id",
    # Set to NULL (Google Sheets formula columns - not available from API)
    "DEPARA_Source", "percentual_comissao", "comissao", "DEPARA_Medium",
    # Added by script
    "hash_key"
]


def get_existing_hashes(cursor) -> Set[str]:
    """
    Get set of existing hash_keys from MySQL table for deduplication
    """
    cursor.execute(f"SELECT hash_key FROM {TABLE_NAME}")
    return {row[0] for row in cursor.fetchall()}


def prepare_record_values(record: Dict) -> tuple:
    """Prepare a record for insertion - returns tuple of values"""
    return (
        # Added by script
        record.get("date"),
        # From API response
        record.get("operacao_codigo"),
        record.get("nsu"),
        record.get("sorteio_id"),
        record.get("sorteio_descricao"),
        record.get("comprador_nome"),
        record.get("comprador_email"),
        record.get("comprador_telefone"),
        record.get("compra_brinde"),
        record.get("afiliado_codigo"),
        record.get("data_venda"),
        record.get("valor_total"),
        record.get("qtd_total"),
        record.get("utm_source"),
        record.get("utm_medium"),
        record.get("utm_campaign"),
        record.get("utm_term"),
        record.get("utm_content"),
        # Added by script
        record.get("operador_id"),
        # Set to NULL (Google Sheets formula columns - not available from API)
        None,  # DEPARA_Source
        None,  # percentual_comissao
        None,  # comissao
        None,  # DEPARA_Medium
        # Added by script
        record.get("hash_key")
    )


def get_insert_sql() -> str:
    """Generate INSERT SQL statement"""
    placeholders = ", ".join(["%s"] * len(INSERT_COLUMNS))
    columns_sql = ", ".join([f"`{col}`" for col in INSERT_COLUMNS])
    return f"INSERT INTO {TABLE_NAME} ({columns_sql}) VALUES ({placeholders})"


def insert_records_with_cursor(cursor, records: List[Dict], batch_size: int = 1000) -> int:
    """Insert records using provided cursor"""
    if not records:
        return 0

    insert_sql = get_insert_sql()
    inserted = 0
    batch = []

    for record in records:
        values = prepare_record_values(record)
        batch.append(values)

        if len(batch) >= batch_size:
            cursor.executemany(insert_sql, batch)
            inserted += len(batch)
            print(f"Inserted {inserted} records...")
            batch = []

    # Insert remaining
    if batch:
        cursor.executemany(insert_sql, batch)
        inserted += len(batch)

    return inserted


# ====== Main Import Function (Airflow Compatible) ======

def fetch_and_prepare_records(
    api_base_url: str,
    api_auth_token: str,
    existing_hashes: Set[str],
    start_date_str: Optional[str] = None,
    end_date_str: Optional[str] = None,
    always_today: bool = True,
    lookback_days: int = 1,
    use_day_by_day: bool = True,
    snowflake_cursor=None,
) -> List[Dict]:
    """
    Fetch records from API and prepare for insertion.
    Returns list of new records (deduplicated).

    This function is designed to be used with Airflow operators.

    Args:
        lookback_days: Number of days to look back (default 1 = yesterday + today).
                       This ensures late-arriving records from previous day are captured.
        use_day_by_day: If True and lookback_days > 0, iterate day-by-day instead of
                        making a single API call with the entire date range. This works
                        around API limitations with large date ranges.
        snowflake_cursor: Snowflake cursor for control table operations. If provided,
                          uses the Snowflake control table (STG.APOSTA_PREMIA.PLAY55_PAIRS_CONTROL)
                          to track which pairs were active on each date.
                          This ensures historical backfills don't miss data from pairs
                          that are no longer active.
    """
    # 1) Create API client
    api_client = Play55ApiClient(api_base_url, api_auth_token)

    # 2) Ensure control table exists in Snowflake if cursor is provided
    use_control_table = snowflake_cursor is not None
    if use_control_table:
        ensure_pairs_control_table(snowflake_cursor)
        print("Control table mode: ENABLED (Snowflake: STG.APOSTA_PREMIA.PLAY55_PAIRS_CONTROL)")
    else:
        print("Control table mode: DISABLED (using current API pairs only)")

    # 3) Determine date ranges to query
    if use_day_by_day and lookback_days > 0:
        # Day-by-day mode: generate separate date range for each day
        daily_ranges = generate_daily_date_ranges(
            lookback_days=lookback_days,
            include_today=always_today
        )
        print(f"Using DAY-BY-DAY mode: {len(daily_ranges)} days to process")
        for i, (start, _) in enumerate(daily_ranges, 1):
            print(f"  Day {i}: {start.split('T')[0]}")
    else:
        # Single range mode (original behavior)
        data_inicio, data_fim = get_date_range(
            start_date_str=start_date_str,
            end_date_str=end_date_str,
            always_today=always_today,
            lookback_days=lookback_days
        )
        daily_ranges = [(data_inicio, data_fim)]
        print(f"Using SINGLE-RANGE mode: {data_inicio} -> {data_fim}")

    # 4) If not using control table, get pairs once (original behavior)
    if not use_control_table:
        pairs = api_client.get_active_pairs()
        if not pairs:
            print("No active pairs found.")
            return []
        print(f"Using {len(pairs)} pairs from current API response...")

    # 5) Fetch sales for each day
    all_sales = []
    total_days = len(daily_ranges)

    for day_idx, (data_inicio, data_fim) in enumerate(daily_ranges, 1):
        day_date = data_inicio.split("T")[0]

        if total_days > 1:
            print(f"\n{'=' * 60}")
            print(f"DAY {day_idx}/{total_days}: {day_date}")
            print(f"{'=' * 60}")

        # Get pairs for this specific day (control table or API)
        if use_control_table:
            pairs = get_pairs_for_date(snowflake_cursor, api_client, day_date, store_to_control=True)
            if not pairs:
                print(f"  No pairs found for {day_date}, skipping...")
                continue
            print(f"  Using {len(pairs)} pairs for {day_date}")
        # else: pairs already set above (once for all days)

        day_records_count = 0

        for pair in pairs:
            so_id = pair["sorteio_id"]
            candidates = pair["operador_id_candidates"]

            for op_id in candidates:
                records = api_client.fetch_sales_for_pair(op_id, so_id, data_inicio, data_fim)
                if records:
                    if total_days > 1:
                        print(f"  OK: op={op_id}/so={so_id} -> {len(records)} records")
                    else:
                        print(f"OK: operador_id={op_id} / sorteio_id={so_id} -> {len(records)} records")
                    all_sales.extend(records)
                    day_records_count += len(records)
                    break  # Found valid operator for this raffle
                else:
                    if total_days > 1:
                        print(f"  No data for op={op_id}/so={so_id}")
                    else:
                        print(f"No data for op={op_id} / so={so_id}. Trying next candidate...")

            time.sleep(0.1)  # Small delay between pairs

        if total_days > 1:
            print(f"  Day {day_date} total: {day_records_count} records")

        # Small delay between days to avoid rate limiting
        if day_idx < total_days:
            time.sleep(0.15)

    if not all_sales:
        print("No sales found in the date range.")
        return []

    print(f"\nTotal sales fetched: {len(all_sales)}")

    # 6) Deduplicate - compute hash and filter out existing
    new_records = []
    local_hashes = set(existing_hashes)  # Copy to avoid modifying original

    for sale in all_sales:
        # Compute hash key using stable key logic
        stable_key = compute_stable_key(sale)
        hash_key = sha256_hex(stable_key)

        if hash_key not in local_hashes:
            local_hashes.add(hash_key)  # Prevent duplicates within this batch
            sale["hash_key"] = hash_key
            # Use actual sale date (data_venda) if available, otherwise fall back to today
            sale_date = sale.get("data_venda")
            if sale_date:
                # Extract date part from data_venda (format: "2025-12-17 23:15" or "2025-12-17")
                # Convert to MM/DD/YYYY format to match existing table data
                date_str = str(sale_date).split("T")[0].split(" ")[0]  # "2025-12-17"
                parts = date_str.split("-")  # ["2025", "12", "17"]
                if len(parts) == 3:
                    sale["date"] = f"{parts[1]}/{parts[2]}/{parts[0]}"  # "12/17/2025"
                else:
                    sale["date"] = date_str
            else:
                sale["date"] = datetime.now().strftime("%m/%d/%Y")
            new_records.append(sale)

    print(f"New records to insert: {len(new_records)}")
    print(f"Duplicates skipped: {len(all_sales) - len(new_records)}")

    return new_records
