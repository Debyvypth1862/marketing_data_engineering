#!/usr/bin/env python3
"""
Play55 Quality Check Module

Compares record counts between Google Sheet (FB/TT_Play55_endpoint) and Snowflake
to validate data consistency after pipeline runs.
"""

import json
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import gspread
from google.oauth2.service_account import Credentials


SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/drive.readonly'
]


def get_gsheet_client(credentials_json: str) -> gspread.Client:
    """
    Create gspread client from service account credentials JSON.

    Args:
        credentials_json: JSON string containing service account credentials

    Returns:
        Authenticated gspread client
    """
    creds = Credentials.from_service_account_info(
        json.loads(credentials_json),
        scopes=SCOPES
    )
    return gspread.authorize(creds)


def get_gsheet_record_counts(
    client: gspread.Client,
    spreadsheet_id: str,
    sheet_name: str,
    date_column: str = 'data',
    days_back: Optional[int] = None
) -> Dict[str, int]:
    """
    Get record counts by date from Google Sheet.

    Args:
        client: Authenticated gspread client
        spreadsheet_id: Google Spreadsheet ID (from the URL)
        sheet_name: Name of the worksheet within the spreadsheet
        date_column: Column name containing the date values
        days_back: Optional - only count records from the last N days (same as Snowflake filter)

    Returns:
        Dictionary mapping date strings to record counts
    """
    spreadsheet = client.open_by_key(spreadsheet_id)
    worksheet = spreadsheet.worksheet(sheet_name)
    records = worksheet.get_all_records()

    # Calculate cutoff date if days_back is specified
    cutoff_date = None
    if days_back is not None:
        cutoff_date = (datetime.now() - timedelta(days=days_back)).date()

    counts: Dict[str, int] = {}
    for record in records:
        date_val = record.get(date_column, '')
        if date_val:
            date_str = str(date_val).strip()
            if not date_str:
                continue

            # Normalize the date format to MM/DD/YYYY for consistent comparison
            normalized_date = normalize_date_format(date_str)
            if not normalized_date:
                continue  # Skip records with unparseable dates

            # If days_back filter is specified, check if date is within range
            if cutoff_date:
                try:
                    record_date = datetime.strptime(normalized_date, '%m/%d/%Y').date()
                    if record_date < cutoff_date:
                        continue  # Skip records older than cutoff
                except ValueError:
                    continue  # Skip records with unparseable dates

            # Use normalized date as key to match Snowflake format
            counts[normalized_date] = counts.get(normalized_date, 0) + 1

    return counts


def get_snowflake_record_counts(cursor, days_back: int = 7) -> Dict[str, int]:
    """
    Get record counts by date from Snowflake STG table.

    Args:
        cursor: Snowflake cursor
        days_back: Number of days to look back from today

    Returns:
        Dictionary mapping date strings (MM/DD/YYYY format) to record counts
    """
    query = """
    SELECT
        TO_CHAR(DATA_VENDA::DATE, 'MM/DD/YYYY') as sale_date,
        COUNT(*) as cnt
    FROM STG.APOSTA_PREMIA.PLAY55_VENDAS
    WHERE DATA_VENDA >= DATEADD(day, -%s, CURRENT_DATE())
    GROUP BY DATA_VENDA::DATE
    ORDER BY DATA_VENDA::DATE
    """
    cursor.execute(query, (days_back,))
    return {row[0]: row[1] for row in cursor.fetchall()}


def normalize_date_format(date_str: str) -> Optional[str]:
    """
    Normalize various date formats to MM/DD/YYYY for comparison.

    Args:
        date_str: Date string in various formats

    Returns:
        Date string in MM/DD/YYYY format, or None if parsing fails
    """
    date_formats = [
        '%m/%d/%Y',      # MM/DD/YYYY
        '%Y-%m-%d',      # YYYY-MM-DD
        '%d/%m/%Y',      # DD/MM/YYYY
        '%m-%d-%Y',      # MM-DD-YYYY
        '%Y/%m/%d',      # YYYY/MM/DD
    ]

    for fmt in date_formats:
        try:
            parsed = datetime.strptime(date_str.strip(), fmt)
            return parsed.strftime('%m/%d/%Y')
        except ValueError:
            continue

    return None


def compare_counts(
    gsheet_counts: Dict[str, int],
    snowflake_counts: Dict[str, int],
    normalize_dates: bool = True
) -> Dict:
    """
    Compare record counts between Google Sheet and Snowflake.

    Args:
        gsheet_counts: Record counts from Google Sheet
        snowflake_counts: Record counts from Snowflake
        normalize_dates: Whether to normalize date formats before comparison

    Returns:
        Dictionary containing comparison results
    """
    # Normalize dates if requested
    if normalize_dates:
        normalized_gsheet = {}
        for date_str, count in gsheet_counts.items():
            normalized = normalize_date_format(date_str)
            if normalized:
                normalized_gsheet[normalized] = normalized_gsheet.get(normalized, 0) + count
            else:
                # Keep original if normalization fails
                normalized_gsheet[date_str] = normalized_gsheet.get(date_str, 0) + count
        gsheet_counts = normalized_gsheet

        normalized_snowflake = {}
        for date_str, count in snowflake_counts.items():
            normalized = normalize_date_format(date_str)
            if normalized:
                normalized_snowflake[normalized] = normalized_snowflake.get(normalized, 0) + count
            else:
                normalized_snowflake[date_str] = normalized_snowflake.get(date_str, 0) + count
        snowflake_counts = normalized_snowflake

    # Get all dates from both sources
    all_dates = set(gsheet_counts.keys()) | set(snowflake_counts.keys())

    discrepancies: List[Dict] = []
    matches: List[Dict] = []

    for date in sorted(all_dates):
        gs_count = gsheet_counts.get(date, 0)
        sf_count = snowflake_counts.get(date, 0)
        diff = sf_count - gs_count

        record = {
            'date': date,
            'gsheet_count': gs_count,
            'snowflake_count': sf_count,
            'difference': diff,
            'pct_diff': round((diff / gs_count * 100), 2) if gs_count > 0 else (100.0 if sf_count > 0 else 0.0)
        }

        if diff != 0:
            discrepancies.append(record)
        else:
            matches.append(record)

    total_gsheet = sum(gsheet_counts.values())
    total_snowflake = sum(snowflake_counts.values())

    return {
        'total_dates_checked': len(all_dates),
        'dates_matched': len(matches),
        'dates_with_discrepancies': len(discrepancies),
        'total_gsheet_records': total_gsheet,
        'total_snowflake_records': total_snowflake,
        'total_difference': total_snowflake - total_gsheet,
        'discrepancies': discrepancies,
        'matches': matches,
        'status': 'PASS' if len(discrepancies) == 0 else 'WARN'
    }


def format_quality_report(result: Dict) -> str:
    """
    Format the quality check result into a readable report.

    Args:
        result: Output from compare_counts()

    Returns:
        Formatted string report
    """
    lines = [
        "=" * 60,
        "QUALITY CHECK REPORT: Google Sheet vs Snowflake",
        "=" * 60,
        f"Status: {result['status']}",
        f"Total dates checked: {result['total_dates_checked']}",
        f"Dates matched: {result['dates_matched']}",
        f"Dates with discrepancies: {result['dates_with_discrepancies']}",
        "",
        f"Total GSheet records: {result['total_gsheet_records']:,}",
        f"Total Snowflake records: {result['total_snowflake_records']:,}",
        f"Total difference: {result['total_difference']:+,}",
        "=" * 60,
    ]

    # Combine all dates (discrepancies + matches) and sort chronologically
    all_dates = result['discrepancies'] + result['matches']
    if all_dates:
        # Sort by date
        all_dates_sorted = sorted(all_dates, key=lambda x: x['date'])

        lines.append("")
        lines.append("ALL DATES:")
        lines.append("-" * 60)
        lines.append(f"{'Date':<15} {'GSheet':>10} {'Snowflake':>12} {'Diff':>10} {'%':>8}")
        lines.append("-" * 60)

        for d in all_dates_sorted:
            lines.append(
                f"{d['date']:<15} {d['gsheet_count']:>10,} {d['snowflake_count']:>12,} "
                f"{d['difference']:>+10,} {d['pct_diff']:>7.1f}%"
            )
        lines.append("-" * 60)

    lines.append("")
    lines.append("=" * 60)

    return "\n".join(lines)


def format_slack_message(result: Dict) -> Dict:
    """
    Format the quality check result as a Slack Block Kit message.

    Args:
        result: Output from compare_counts()

    Returns:
        Slack message payload dict
    """
    status_emoji = ":white_check_mark:" if result['status'] == 'PASS' else ":warning:"
    header = f"{status_emoji} Play55 Quality Check: *{result['status']}*"

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "Play55 Data Quality Check"}
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": header
            }
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Dates Checked:*\n{result['total_dates_checked']}"},
                {"type": "mrkdwn", "text": f"*Dates Matched:*\n{result['dates_matched']}"},
                {"type": "mrkdwn", "text": f"*GSheet Records:*\n{result['total_gsheet_records']:,}"},
                {"type": "mrkdwn", "text": f"*Snowflake Records:*\n{result['total_snowflake_records']:,}"},
                {"type": "mrkdwn", "text": f"*Discrepancies:*\n{result['dates_with_discrepancies']}"},
                {"type": "mrkdwn", "text": f"*Total Diff:*\n{result['total_difference']:+,}"},
            ]
        },
    ]

    # Combine all dates (discrepancies + matches) into one continuous list
    all_dates = result['discrepancies'] + result['matches']
    if all_dates:
        # Sort by date
        all_dates_sorted = sorted(all_dates, key=lambda x: x['date'])
        total_dates = len(all_dates_sorted)

        if total_dates <= 20:
            # If 20 or fewer, show all in this message
            lines = ["*All Dates:*", "```"]
            lines.append(f"{'Date':<15} {'GSheet':>8} {'Snowflake':>10} {'Diff':>8}")
            lines.append("-" * 45)
            for d in all_dates_sorted:
                lines.append(
                    f"{d['date']:<15} {d['gsheet_count']:>8,} {d['snowflake_count']:>10,} {d['difference']:>+8,}"
                )
            lines.append("```")
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(lines)}
            })
        else:
            # If more than 20, note that details will follow in separate messages
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Date details will be sent in separate messages below* ({total_dates} total dates)"}
            })

    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [
            {"type": "mrkdwn", "text": f"Run at {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"}
        ]
    })

    return {"blocks": blocks}


def send_slack_notification(webhook_url: str, result: Dict, chunk_size: int = 20) -> bool:
    """
    Send quality check results to a Slack channel via webhook.
    Sends multiple messages if there are too many discrepancies to avoid Slack size limits.

    Args:
        webhook_url: Slack incoming webhook URL
        result: Output from compare_counts()
        chunk_size: Maximum number of discrepancies per message (default: 20)

    Returns:
        True if all messages were sent successfully
    """
    import time

    # Send main summary message
    payload = format_slack_message(result)
    response = requests.post(
        webhook_url,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=10,
    )

    if response.status_code != 200 or response.text != "ok":
        print(f"Failed to send main Slack notification: {response.status_code} - {response.text}")
        return False

    print("Main Slack notification sent successfully.")

    # If there are more than chunk_size dates total, send them in separate messages
    all_dates = result.get('discrepancies', []) + result.get('matches', [])
    all_dates_sorted = sorted(all_dates, key=lambda x: x['date'])

    if len(all_dates_sorted) > chunk_size:
        total_chunks = (len(all_dates_sorted) + chunk_size - 1) // chunk_size  # Ceiling division

        for chunk_idx in range(total_chunks):
            start_idx = chunk_idx * chunk_size
            end_idx = min(start_idx + chunk_size, len(all_dates_sorted))
            chunk = all_dates_sorted[start_idx:end_idx]

            # Format date chunk message
            lines = [f"*Dates ({start_idx + 1}-{end_idx} of {len(all_dates_sorted)}):*", "```"]
            lines.append(f"{'Date':<15} {'GSheet':>8} {'Snowflake':>10} {'Diff':>8}")
            lines.append("-" * 45)
            for d in chunk:
                lines.append(
                    f"{d['date']:<15} {d['gsheet_count']:>8,} {d['snowflake_count']:>10,} {d['difference']:>+8,}"
                )
            lines.append("```")

            chunk_payload = {
                "blocks": [{
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "\n".join(lines)}
                }]
            }

            # Small delay between messages to avoid rate limiting
            time.sleep(0.5)

            response = requests.post(
                webhook_url,
                json=chunk_payload,
                headers={"Content-Type": "application/json"},
                timeout=10,
            )

            if response.status_code == 200 and response.text == "ok":
                print(f"Sent date chunk {chunk_idx + 1}/{total_chunks}")
            else:
                print(f"Failed to send chunk {chunk_idx + 1}: {response.status_code} - {response.text}")
                return False

    return True
