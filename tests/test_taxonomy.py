import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from governance.check_taxonomy import validate_table  # noqa: E402

GOOD = {"data_owner": "x@y", "domain": "crm", "layer": "bronze", "description": "d", "pii": "low"}
BRONZE_COLS = {"Id", "_batch_id", "_ingested_at", "_source_file"}


def test_valid_bronze_table_passes():
    assert validate_table("bronze", "crm_accounts", GOOD, BRONZE_COLS) == []


def test_bad_bronze_name_is_rejected():
    assert any("name does not match" in p for p in validate_table("bronze", "Accounts", GOOD, BRONZE_COLS))


def test_unknown_domain_prefix_is_rejected():
    assert validate_table("bronze", "hr_employees", GOOD, BRONZE_COLS)


def test_missing_load_metadata_column_is_rejected():
    problems = validate_table("bronze", "crm_accounts", GOOD, {"Id"})
    assert any("_batch_id" in p for p in problems)


def test_missing_owner_is_rejected():
    props = {k: v for k, v in GOOD.items() if k != "data_owner"}
    assert any("data_owner" in p for p in validate_table("bronze", "crm_accounts", props, BRONZE_COLS))


def test_layer_property_must_match_namespace():
    props = dict(GOOD, layer="gold")
    assert any("layer=gold" in p for p in validate_table("bronze", "crm_accounts", props, BRONZE_COLS))


def test_gold_and_silver_patterns():
    gold = dict(GOOD, layer="gold", domain="revenue")
    silver = dict(GOOD, layer="silver")
    assert validate_table("gold", "rpt_revenue_monthly", gold, set()) == []
    assert validate_table("gold", "stg_crm__accounts", gold, set())          # staging does not belong in gold
    assert validate_table("silver", "stg_crm__accounts", silver, set()) == []
    assert validate_table("silver", "ref_country_codes", silver, set()) == []
