#!/usr/bin/env python3
"""process_santander.py – Extração e consolidação de Holdings / Transactions (Santander)
====================================================================================
Este script lê o **primeiro** arquivo `.xlsx` encontrado em
``F:/Automatiza_e_consolida/Calculo receita bancos/Santander/Input``
(Windows‐style path; ajuste em outros SO)
, extrai as sub‑tabelas de *Holdings Current*, *Holdings Past* e *Transactions*,
insere metadados (Portfolio, Tipo de Ativo, Nome do Ativo) e salva um único
arquivo:
``F:/Automatiza_e_consolida/Calculo receita bancos/Santander/Output/
   Santander - Cálculo receita - parte 1.xlsx``

Principais regras de negócio
---------------------------
* **Robustez a variações**: todas as comparações de texto usam
  ``str.upper().strip()``.
* **Portfolio**: varre as 15 primeiras linhas de cada aba e procura “CONTA # ”
  + número.
* **Holdings**: cada sub‑tabela começa em “ISIN” ou “NÚMERO DE CONTA”.
  * *AssetType* = célula [linha‑1, col‑1];
  * *AssetName* = célula [linha,   col‑1].
  * Linhas que começam com palavras‑chave (RESUMO DE ATIVOS, TOTAL, etc.) são
    descartadas.
* **Transactions**: cada sub‑tabela começa em uma linha cujo 1º campo =
  “CARTEIRA”; a própria linha é o cabeçalho. Campo **DETALHES** é decomposto
  em colunas (Tipo, Advice#, AdviceType, Quantidades, AssetName, Data, Moeda).
* **Datas**: qualquer coluna cujo nome contenha “DATA”|"DATE" é normalizada
  p/ formato ``dd/mm/yyyy``; meses em PT‑BR são convertidos (MAI→MAY etc.).
* **Colunas duplicadas** recebem sufixos _1, _2…
* Mensagens «emoji» no terminal mostram o andamento.

Requisitos
~~~~~~~~~~
``pip install pandas openpyxl python-dateutil``
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import List, Tuple, Dict, Any

import pandas as pd
from dateutil import parser as dtparser

###########################################################################
#  PARÂMETROS DE DIRETÓRIO                                                #
###########################################################################
INPUT_DIR  = Path(r"F:/satander-converter/")
OUTPUT_XLS = Path(r"F:/satander-converter/"
                  r"Santander - Cálculo receita - parte 1.xlsx")


UPPER_STRIP = lambda s: str(s).upper().strip() if isinstance(s, str) else ""
KEYWORDS_SKIP = {"RESUMO DE ATIVOS", "NOME DO ATIVO", "RENDA FIXA", "CURTO PRAZO", "TOTAL"}
MONTH_PT_EN = {"JAN": "JAN", "FEV": "FEB", "MAR": "MAR", "ABR": "APR", "MAI": "MAY", "JUN": "JUN", "JUL": "JUL", "AGO": "AUG", "SET": "SEP", "OUT": "OCT", "NOV": "NOV", "DEZ": "DEC"}

RE_ADVICE = re.compile(r"ADVICE#\s*[:#]?\s*-?\s*([A-Z0-9.\-]+)", re.I)
RE_FX_CODE = re.compile(r"\bFX\d{8,}\b", re.I)
RE_GENERIC_ADV = re.compile(r"\b[A-Z]{4,}\d{6,}[A-Z0-9.\-]*\b", re.I)  # pega SCTRSC… DIARSC… etc.
RE_CCY = re.compile(r"\b([A-Z]{3})\b")
RE_SLASH_QTY = re.compile(r"\b(\d[\d,.]*)/\s*(\d[\d,.]*)\b")
RE_QTY_STANDALONE = re.compile(r"\b\d[\d,.]*\b")
RE_QTY_PT1 = re.compile(r"QUANTIDADE\s*1[:\s]+(\d[\d,.]*)", re.I)
RE_QTY_PT2 = re.compile(r"QUANTIDADE\s*2[:\s]+(\d[\d,.]*)", re.I)
RE_DATE_TOKEN = re.compile(r"\b\d{2}[A-Z]{3}\d{2}\b")


def _dedup_columns(cols: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    res: List[str] = []
    for col in (c or "_" for c in cols):
        k = seen.get(col, 0)
        res.append(f"{col}_{k}" if k else col)
        seen[col] = k + 1
    return res


def extract_portfolio_id(df: pd.DataFrame) -> str | None:
    for i in range(min(15, len(df))):
        for v in df.iloc[i].dropna():
            m = re.search(r"CONTA\s*#\s*(\d+)", str(v), re.I)
            if m:
                return m.group(1)
    return None


def normalize_date(v: Any) -> Any:
    if not isinstance(v, str):
        return v
    s = v.strip().upper()
    for pt, en in MONTH_PT_EN.items():
        s = s.replace(pt, en)
    if RE_DATE_TOKEN.fullmatch(s):
        s = f"{s[:2]} {s[2:5]} 20{s[5:]}"
    try:
        return dtparser.parse(s, dayfirst=True).strftime("%d/%m/%Y")
    except Exception:
        return v


def _clean_num(s: str) -> str:
    return s.replace(',', '').lstrip('0') or '0'


def _extract_advice(rest: str) -> Tuple[str | None, str]:
    m = RE_ADVICE.search(rest)
    if m:
        code = m.group(1)
        rest = rest[:m.start()] + rest[m.end():]
        return code.strip(' /-'), rest.strip(' /-')
    m = RE_FX_CODE.search(rest)
    if m:
        code = m.group(0)
        rest = rest[:m.start()] + rest[m.end():]
        return code, rest.strip(' /-')
    m = RE_GENERIC_ADV.search(rest)
    if m:
        code = m.group(0)
        rest = rest[:m.start()] + rest[m.end():]
        return code, rest.strip(' /-')
    return None, rest


def parse_details(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, str):
        return {}
    txt = raw.strip()
    out: Dict[str, Any] = {}
    m_tipo = re.match(r"^[A-Z ]+", txt)
    tipo = m_tipo.group(0).strip() if m_tipo else None
    out['Det_Tipo'] = tipo
    rest = txt[m_tipo.end():].lstrip(' /-').strip() if m_tipo else txt

    advice, rest = _extract_advice(rest)
    if advice:
        out['Det_Advice'] = advice

    qty_end = 0
    m = RE_SLASH_QTY.search(rest)
    if m:
        out['Det_Qty1'], out['Det_Qty2'] = map(_clean_num, m.groups())
        qty_end = m.end()
    else:
        mq1 = RE_QTY_PT1.search(rest)
        if mq1:
            out['Det_Qty1'] = _clean_num(mq1.group(1))
            qty_end = mq1.end()
        mq2 = RE_QTY_PT2.search(rest)
        if mq2:
            out['Det_Qty2'] = _clean_num(mq2.group(1))
            qty_end = max(qty_end, mq2.end())
        if 'SECURITIES' in (tipo or '') and 'Det_Qty1' not in out:
            mnum = RE_QTY_STANDALONE.search(rest)
            if mnum:
                out['Det_Qty1'] = _clean_num(mnum.group(0))
                qty_end = mnum.end()

    remaining = rest[qty_end:].lstrip(' -/').strip()

    ccy = None
    for c in reversed(RE_CCY.findall(remaining or rest)):
        if c in {'USD', 'EUR', 'GBP', 'CHF', 'BRL', 'CAD', 'JPY'}:
            ccy = c
            if remaining.upper().endswith(ccy):
                remaining = remaining[:-len(ccy)].rstrip(' -').strip()
            break
    out['Det_Ccy'] = ccy

    mdt = RE_DATE_TOKEN.search(remaining)
    if mdt:
        out['Det_Date'] = normalize_date(mdt.group(0))
        remaining = remaining.replace(mdt.group(0), '').strip()

    madv = re.search(r'[A-Z ]+ADVICE', txt)
    if madv:
        out['Det_AdviceType'] = madv.group(0).strip()

    out['Det_AssetName'] = remaining or None
    return out


def parse_holdings_sheet(df: pd.DataFrame, portfolio: str) -> List[pd.DataFrame]:
    headers: List[Tuple[int, int]] = []
    for r, row in df.iterrows():
        for c, cell in row.items():
            if UPPER_STRIP(cell) in {"ISIN", "NÚMERO DE CONTA"}:
                headers.append((r, c))
    subtables: List[pd.DataFrame] = []
    for i, (r, c) in enumerate(headers):
        next_r = headers[i + 1][0] if i + 1 < len(headers) else len(df)
        header_row = df.iloc[r]
        data = df.iloc[r + 1 : next_r].copy().dropna(how="all")
        data = data[~data.iloc[:, 0].apply(lambda x: UPPER_STRIP(x) in KEYWORDS_SKIP)]
        if data.empty:
            continue
        data.columns = _dedup_columns([str(v).strip() for v in header_row])[: len(data.columns)]
        asset_type = UPPER_STRIP(df.iat[r - 1, c - 1]) if r > 0 and c > 0 else None
        asset_name = str(df.iat[r, c - 1]).strip() if c > 0 else None
        data.insert(0, "Portfolio", portfolio)
        data.insert(1, "AssetType", asset_type)
        data.insert(2, "AssetName", asset_name)
        for col in data.columns:
            if re.search(r"DATA|DATE", col, re.I):
                data[col] = data[col].apply(normalize_date)
        subtables.append(data)
    return subtables


def parse_transactions_sheet(df: pd.DataFrame, portfolio: str) -> List[pd.DataFrame]:
    idxs = [i for i, v in df.iloc[:, 0].items() if UPPER_STRIP(v) == "CARTEIRA"]
    out: List[pd.DataFrame] = []
    for i, start in enumerate(idxs):
        end = idxs[i + 1] if i + 1 < len(idxs) else len(df)
        header = df.iloc[start]
        body = df.iloc[start + 1 : end].copy().dropna(how="all")
        if body.empty:
            continue
        body.columns = _dedup_columns([str(v).strip() for v in header])[:len(body.columns)]
        det_col = next((c for c in body.columns if UPPER_STRIP(c) in {"DETALHES", "DETAILS"}), None)
        if det_col:
            details_expanded = body[det_col].apply(parse_details).apply(pd.Series)
            body = pd.concat([body, details_expanded], axis=1)
        for col in body.columns:
            if re.search(r"DATA|DATE", col, re.I):
                body[col] = body[col].apply(normalize_date)
        body.insert(0, "Portfolio", portfolio)
        out.append(body)
    return out

# ---------------------------------------------------------------------------
#  BUILD & SAVE                                                             #
# ---------------------------------------------------------------------------

def build_dataframes(xlsx: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    wb = pd.ExcelFile(xlsx)
    df_curr_list: List[pd.DataFrame] = []
    df_past_list: List[pd.DataFrame] = []
    df_tx_list: List[pd.DataFrame] = []
    df_corporate_list: List[pd.DataFrame] = []
    for sheet in wb.sheet_names:
        print("🔍", sheet)
        df_sheet = wb.parse(sheet, header=None, dtype=object)
        portfolio = extract_portfolio_id(df_sheet) or "UNKNOWN"
        name_u = sheet.upper()
        if "CORPORATE ACTIVITY" in name_u:
            df_corporate_list.extend(_extract_advice_code(df_sheet,portfolio))
        if "HOLDING" in name_u and "CURRENT" in name_u:
            df_curr_list.extend(parse_holdings_sheet(df_sheet, portfolio))
        elif "HOLDING" in name_u and "PAST" in name_u:
            df_past_list.extend(parse_holdings_sheet(df_sheet, portfolio))
        elif "TRANSACTION" in name_u:
            df_tx_list.extend(parse_transactions_sheet(df_sheet, portfolio))
        else:
            print("⚠️  aba ignorada")
    df_corporate = pd.concat(df_corporate_list,ignore_index=True) if df_corporate_list else pd.DataFrame()
    df_curr = pd.concat(df_curr_list, ignore_index=True) if df_curr_list else pd.DataFrame()
    df_past = pd.concat(df_past_list, ignore_index=True) if df_past_list else pd.DataFrame()
    df_tx   = pd.concat(df_tx_list,   ignore_index=True) if df_tx_list   else pd.DataFrame()
    return df_curr, df_past, df_tx


def save_to_excel(out: Path, curr: pd.DataFrame, past: pd.DataFrame, tx: pd.DataFrame):
    out.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out, engine="openpyxl", date_format="DD/MM/YYYY") as w:
        curr.to_excel(w, "Holdings_Current", index=False)
        past.to_excel(w, "Holdings_Past", index=False)
        tx.to_excel(w, "Transactions", index=False)

# ---------------------------------------------------------------------------
#  MAIN                                                                     #
# ---------------------------------------------------------------------------

def main():
    try:
        xls = next(p for p in INPUT_DIR.glob("*.xlsx") if not p.name.startswith("~"))
    except StopIteration:
        sys.exit(f"Nenhum .xlsx em {INPUT_DIR}")
    print("📑", xls.name)
    curr, past, tx = build_dataframes(xls)
    print("💾 salvando →", OUTPUT_XLS.name)
    save_to_excel(OUTPUT_XLS, curr, past, tx)
    print("✅ linhas Curr/Past/Tx:", len(curr), len(past), len(tx))

if __name__ == "__main__":
    main()
