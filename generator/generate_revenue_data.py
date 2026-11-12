"""Generate messy, realistic revenue data for a B2B travel-tech SaaS company.

Sources (as they would land from real systems):
  crm      (Salesforce-style CSV export): accounts, opportunities
  billing  (ERP / billing API, JSON lines): invoices, payments
  finance  (treasury CSV): fx_rates

Three batches land in data/landing/<batch>/<domain>/<entity>.<ext>:
  batch_001  history up to 2026-04           (clean-ish, with duplicate accounts and messy countries)
  batch_002  2026-05 and 2026-06             (billing adds a new `tax_amount` column -> schema evolution)
  batch_003  a broken export                 (null keys, negative amounts -> must be blocked by the audit)
"""

from __future__ import annotations

import csv
import json
import random
import shutil
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LANDING = ROOT / "data" / "landing"
rng = random.Random(7)

PRODUCTS = {  # product: monthly list price in USD
    "Rate Intelligence": 1800,
    "Channel Manager": 1200,
    "Hotel Martech": 950,
    "Air Pricing": 3200,
}
SEGMENTS = {"Enterprise": (6, 14), "Mid-Market": (2, 5), "SMB": (1, 2)}  # seats multiplier range
INDUSTRIES = ["Hotel Chain", "Independent Hotel", "OTA", "Airline", "Car Rental", "Cruise"]
COUNTRY_VARIANTS = {
    "IN": ["IN", "India", "india", "IND"],
    "US": ["US", "USA", "United States", "U.S."],
    "GB": ["GB", "UK", "United Kingdom"],
    "AE": ["AE", "UAE", "United Arab Emirates"],
    "SG": ["SG", "Singapore"],
    "DE": ["DE", "Germany"],
    "ES": ["ES", "Spain"],
}
CURRENCY_BY_COUNTRY = {"IN": "INR", "US": "USD", "GB": "GBP", "AE": "AED", "SG": "SGD", "DE": "EUR", "ES": "EUR"}
FX_BASE = {"USD": 1.0, "INR": 0.0119, "GBP": 1.27, "AED": 0.2723, "SGD": 0.74, "EUR": 1.08}
STAGES = ["Prospecting", "Qualification", "Proposal", "Negotiation", "Closed Won", "Closed Lost"]
OWNERS = ["Aarav Mehta", "Priya Nair", "Daniel Ortiz", "Sara Khan", "Liam Walsh", "Mei Tan"]
NAME_A = ["Azure", "Coral", "Summit", "Harbor", "Lotus", "Crescent", "Atlas", "Meridian", "Saffron", "Nimbus",
          "Evergreen", "Golden", "Royal", "Blue", "Sterling", "Orchid", "Pioneer", "Vista", "Monsoon", "Polar"]
NAME_B = ["Hotels", "Resorts", "Travels", "Airways", "Stays", "Hospitality", "Journeys", "Cruises", "Rentals", "Inns"]

START, CUTOFF_1, END = date(2025, 1, 1), date(2026, 4, 30), date(2026, 6, 30)


def month_starts(a: date, b: date):
    d = date(a.year, a.month, 1)
    while d <= b:
        yield d
        d = date(d.year + (d.month == 12), d.month % 12 + 1, 1)


def build_world():
    accounts, opps, subs = [], [], []
    used_names: set[str] = set()
    for i in range(1, 181):
        cc = rng.choice(list(COUNTRY_VARIANTS))
        seg = rng.choices(list(SEGMENTS), weights=[2, 4, 5])[0]
        name = f"{rng.choice(NAME_A)} {rng.choice(NAME_B)}"
        while name in used_names:  # real companies have distinct names
            name = f"{rng.choice(NAME_A)} {rng.choice(NAME_A)} {rng.choice(NAME_B)}"
        used_names.add(name)
        created = START + timedelta(days=rng.randint(0, 420))
        acc = {
            "Id": f"001{i:07d}",
            "Name": name,
            "Industry": rng.choice(INDUSTRIES),
            "BillingCountry": rng.choice(COUNTRY_VARIANTS[cc]),
            "Segment__c": seg,
            "OwnerName": rng.choice(OWNERS),
            "CreatedDate": created.isoformat(),
            "_cc": cc,
        }
        accounts.append(acc)
        dup_id = None
        # ~8% of accounts get a duplicate record created by a different rep (classic CRM mess)
        if rng.random() < 0.08:
            dup = dict(acc)
            dup["Id"] = f"001{i + 5000:07d}"
            dup["Name"] = rng.choice([name.upper(), name + " Ltd", name.replace(" ", "  "), name + "."])
            dup["BillingCountry"] = rng.choice(COUNTRY_VARIANTS[cc])
            dup["OwnerName"] = rng.choice(OWNERS)
            dup["CreatedDate"] = (created + timedelta(days=rng.randint(5, 90))).isoformat()
            accounts.append(dup)
            dup_id = dup["Id"]

        # opportunities: 1-3 per account
        for k in range(rng.randint(1, 3)):
            product = rng.choice(list(PRODUCTS))
            seats = rng.randint(*SEGMENTS[seg])
            opp_created = created + timedelta(days=rng.randint(0, 120))
            close = opp_created + timedelta(days=rng.randint(20, 150))
            if close > END:
                stage = rng.choice(STAGES[:4])  # still open
            else:
                stage = rng.choices(["Closed Won", "Closed Lost"], weights=[45, 55])[0]
            annual = PRODUCTS[product] * seats * 12
            opp = {
                "Id": f"006{i:05d}{k}",
                # deals logged by the other rep sit on the duplicate record
                "AccountId": dup_id if dup_id and rng.random() < 0.5 else acc["Id"],
                "Name": f"{name} - {product}",
                "StageName": stage,
                "Amount": round(annual * rng.uniform(0.85, 1.15), 2),
                "CurrencyIsoCode": "USD",
                "CloseDate": close.isoformat(),
                "Product__c": product,
                "CreatedDate": opp_created.isoformat(),
            }
            opps.append(opp)
            if stage == "Closed Won":
                subs.append({"account": dict(acc, Id=opp["AccountId"]), "product": product, "mrr_usd": opp["Amount"] / 12, "start": close})
    return accounts, opps, subs


def build_billing(subs):
    invoices, payments = [], []
    n = 0
    for s in subs:
        acc, cc = s["account"], s["account"]["_cc"]
        cur = CURRENCY_BY_COUNTRY[cc]
        mrr = s["mrr_usd"]
        churn_month = None
        if rng.random() < 0.15:
            churn_month = s["start"] + timedelta(days=rng.randint(120, 420))
        for m in month_starts(s["start"] + timedelta(days=31), END):
            if churn_month and m >= churn_month:
                break
            # expansions and contractions over time
            r = rng.random()
            if r < 0.04:
                mrr *= rng.uniform(1.2, 1.6)
            elif r < 0.06:
                mrr *= rng.uniform(0.6, 0.85)
            n += 1
            amount_local = round(mrr / FX_BASE[cur], 2)
            inv_date = m
            due = inv_date + timedelta(days=30)
            inv = {
                "invoice_id": f"INV-{n:06d}",
                "account_ref": acc["Id"],
                "product": s["product"],
                "invoice_date": inv_date.isoformat(),
                "due_date": due.isoformat(),
                "period_start": m.isoformat(),
                "period_end": (date(m.year + (m.month == 12), m.month % 12 + 1, 1) - timedelta(days=1)).isoformat(),
                "currency": cur,
                "amount": amount_local,
                "status": "issued",
            }
            invoices.append(inv)
            # most invoices get paid, some late, some not yet
            p = rng.random()
            if inv_date < date(2026, 5, 15) and p < 0.93:
                paid = due + timedelta(days=rng.randint(-20, 45))
                if paid <= END:
                    payments.append({
                        "payment_id": f"PAY-{n:06d}",
                        "invoice_id": inv["invoice_id"],
                        "paid_at": paid.isoformat() + "T10:00:00Z",
                        "currency": cur,
                        "amount": amount_local,
                        "method": rng.choice(["wire", "card", "ach"]),
                    })
    return invoices, payments


def build_fx():
    rows = []
    for m in month_starts(START, END):
        for cur, base in FX_BASE.items():
            drift = 1 + rng.uniform(-0.03, 0.03) if cur != "USD" else 1
            rows.append({"rate_month": m.isoformat(), "currency": cur, "usd_per_unit": round(base * drift, 6)})
    return rows


def write_csv(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def write_jsonl(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def main():
    if LANDING.exists():
        shutil.rmtree(LANDING)
    accounts, opps, subs = build_world()
    invoices, payments = build_billing(subs)
    fx = build_fx()

    def before(d: str) -> bool:
        return date.fromisoformat(d[:10]) <= CUTOFF_1

    b1, b2, b3 = LANDING / "batch_001", LANDING / "batch_002", LANDING / "batch_003"

    write_csv(b1 / "crm" / "accounts.csv", [a for a in accounts if before(a["CreatedDate"])])
    write_csv(b1 / "crm" / "opportunities.csv", [o for o in opps if before(o["CreatedDate"])])
    write_jsonl(b1 / "billing" / "invoices.jsonl", [i for i in invoices if before(i["invoice_date"])])
    write_jsonl(b1 / "billing" / "payments.jsonl", [p for p in payments if before(p["paid_at"])])
    write_csv(b1 / "finance" / "fx_rates.csv", [r for r in fx if before(r["rate_month"])])

    # batch 2: new records + billing system upgrade adds tax_amount (schema change)
    new_inv = [i for i in invoices if not before(i["invoice_date"])]
    for i in new_inv:
        i["tax_amount"] = round(i["amount"] * 0.18, 2) if i["currency"] == "INR" else 0.0
    write_csv(b2 / "crm" / "accounts.csv", [a for a in accounts if not before(a["CreatedDate"])] or accounts[:1])
    write_csv(b2 / "crm" / "opportunities.csv", [o for o in opps if not before(o["CreatedDate"])] or opps[:1])
    write_jsonl(b2 / "billing" / "invoices.jsonl", new_inv)
    write_jsonl(b2 / "billing" / "payments.jsonl", [p for p in payments if not before(p["paid_at"])])
    write_csv(b2 / "finance" / "fx_rates.csv", [r for r in fx if not before(r["rate_month"])])

    # batch 3: a broken billing export
    broken = [dict(i, invoice_id=None) for i in new_inv[:5]] + [dict(i, amount=-abs(i["amount"])) for i in new_inv[5:10]]
    write_jsonl(b3 / "billing" / "invoices.jsonl", broken)

    print(f"accounts={len(accounts)} opportunities={len(opps)} invoices={len(invoices)} payments={len(payments)} fx={len(fx)}")
    for b in (b1, b2, b3):
        print(b.name, sorted(str(p.relative_to(b)) for p in b.rglob("*.*")))


if __name__ == "__main__":
    main()
