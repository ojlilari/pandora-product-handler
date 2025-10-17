"""Streamlit app for converting Pandora master data into Matrixify imports.

This script implements the workflow described in the developer manual:

* Upload Pandora master Excel and optional Shopify Matrixify export.
* Allow users to verify/override column mappings with resilient reconciliation.
* Generate Matrixify-compatible Excel outputs for new products, safe updates,
  removals, sync reports, and attention-required rows.

The implementation emphasises clarity, resilience against header drift, and a
single source of truth for the transformation logic so the UI and batch
processing share behaviour.
"""

from __future__ import annotations

import io
import math
import re
import unicodedata
import zipfile
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import streamlit as st


# ---------------------------------------------------------------------------
# Column alias configuration
# ---------------------------------------------------------------------------


def _aliases(*values: str) -> Tuple[str, ...]:
    """Utility to declare alias tuples while keeping the mapping readable."""

    return tuple(values)


MASTER_ALIASES: Dict[str, Tuple[str, ...]] = {
    "sku": _aliases("itemid", "item id", "sku", "variant sku"),
    "design_variation": _aliases(
        "design variation",
        "design name",
        "collection name",
        "design",
    ),
    "retail_price": _aliases(
        "lrrp eur new",
        "lrrp",
        "retail price",
        "price",
        "fin rrp eur",
    ),
    "wholesale_price": _aliases(
        "whs eur new",
        "whs",
        "cost",
        "wholesale price",
        "fin whs eur",
    ),
    "title_en": _aliases(
        "article description en",
        "description en",
        "name en",
        "title en",
    ),
    "title_fi": _aliases(
        "article description fi",
        "description fi",
        "name fi",
        "title fi",
    ),
    "long_description_fi": _aliases(
        "long description fi",
        "longdescription fi",
        "long description (fi)",
    ),
    "short_name_en": _aliases(
        "name en (short)",
        "short name en",
        "short description en",
    ),
    "barcode": _aliases("sales barcode", "barcode", "ean"),
    "weight_kg": _aliases(
        "net weight (kg.)",
        "net weight (kg)",
        "net weight",
        "weight kg",
    ),
}


SHOPIFY_ALIASES: Dict[str, Tuple[str, ...]] = {
    "sku": _aliases("variant sku [id]", "variant sku", "sku"),
    "handle": _aliases("handle"),
}


REQUIRED_MASTER_FIELDS = ("sku", "design_variation", "retail_price", "wholesale_price", "title_en")


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------


def normalise_header(value: str) -> str:
    """Normalise a header for resilient matching."""

    value = unicodedata.normalize("NFKD", str(value)).strip().lower()
    value = re.sub(r"\s+", " ", value)
    return value


def build_reverse_alias_map(alias_map: Dict[str, Sequence[str]]) -> Dict[str, str]:
    reverse: Dict[str, str] = {}
    for logical, aliases in alias_map.items():
        for alias in aliases:
            reverse[normalise_header(alias)] = logical
    return reverse


MASTER_REVERSE_ALIASES = build_reverse_alias_map(MASTER_ALIASES)
SHOPIFY_REVERSE_ALIASES = build_reverse_alias_map(SHOPIFY_ALIASES)


def auto_detect_mapping(columns: Iterable[str], alias_map: Dict[str, Sequence[str]]) -> Dict[str, Optional[str]]:
    """Auto-detect the first matching column for each logical field."""

    normalised_to_actual = {normalise_header(col): col for col in columns}
    mapping: Dict[str, Optional[str]] = {logical: None for logical in alias_map.keys()}
    for logical, aliases in alias_map.items():
        for alias in aliases:
            candidate = normalised_to_actual.get(normalise_header(alias))
            if candidate is not None:
                mapping[logical] = candidate
                break
    return mapping


def reconcile_selection(selection: Dict[str, Optional[str]], columns: Sequence[str]) -> Dict[str, Optional[str]]:
    """Reconcile user selections with actual DataFrame columns.

    Handles case differences and trailing whitespace gracefully by resolving the
    selection via normalised header comparisons.
    """

    normalised_to_actual = {normalise_header(col): col for col in columns}
    resolved: Dict[str, Optional[str]] = {}
    for logical, chosen in selection.items():
        if chosen is None:
            resolved[logical] = None
            continue
        normalised = normalise_header(chosen)
        resolved[logical] = normalised_to_actual.get(normalised, chosen)
    return resolved


def ensure_required_fields(mapping: Dict[str, Optional[str]], required: Sequence[str]) -> List[str]:
    missing = [field for field in required if not mapping.get(field)]
    return missing


def slugify(text: str) -> str:
    """Generate a Shopify-compatible handle slug."""

    text = unicodedata.normalize("NFKD", str(text)).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text).strip("-")
    text = re.sub(r"-+", "-", text)
    return text.lower()


def format_decimal(value: Optional[float]) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    return f"{value:.2f}"


def format_grams(value: Optional[float]) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    return str(int(round(value)))


def html_paragraph(text: str) -> str:
    text = str(text).strip()
    return f"<p>{text}</p>" if text else ""


@dataclass
class Settings:
    option1_name: str = "Koko"
    fi_title_fallback: bool = True


@dataclass
class TransformationResult:
    new_products: pd.DataFrame
    updates_safe: pd.DataFrame
    to_remove: pd.DataFrame
    needs_attention: pd.DataFrame
    sync_report: Dict[str, pd.DataFrame]
    metrics: Dict[str, int]
    preflight_notes: List[str]


# ---------------------------------------------------------------------------
# Core transformation logic
# ---------------------------------------------------------------------------


def clean_text_series(series: pd.Series) -> pd.Series:
    """Return a stripped string Series with placeholder nulls removed."""

    text = series.fillna("").astype(str).str.strip()
    return text.mask(text.str.lower().isin({"nan", "none"}), "")


def prepare_master_dataframe(raw: pd.DataFrame, mapping: Dict[str, Optional[str]], settings: Settings) -> pd.DataFrame:
    df = raw.copy()
    canonical = {}

    def col(name: str) -> pd.Series:
        column_name = mapping.get(name)
        return df[column_name] if column_name else pd.Series([None] * len(df))

    canonical["sku"] = clean_text_series(col("sku"))
    canonical["design_variation"] = clean_text_series(col("design_variation"))
    canonical["retail_price"] = pd.to_numeric(col("retail_price"), errors="coerce")
    canonical["wholesale_price"] = pd.to_numeric(col("wholesale_price"), errors="coerce")
    canonical["title_en"] = clean_text_series(col("title_en"))
    canonical["title_fi"] = clean_text_series(col("title_fi"))
    canonical["long_description_fi"] = clean_text_series(col("long_description_fi"))
    canonical["short_name_en"] = clean_text_series(col("short_name_en"))
    canonical["barcode"] = clean_text_series(col("barcode"))
    canonical["weight_kg"] = pd.to_numeric(col("weight_kg"), errors="coerce")

    master = pd.DataFrame(canonical)
    master = master[master["sku"].astype(bool)]

    master["stem"] = master["sku"].str.split("-", n=1).str[0]
    master["suffix"] = master["sku"].str.split("-", n=1).str[1]
    master["has_hyphen"] = master["sku"].str.contains("-", na=False)

    master["design_key"] = master["design_variation"].replace({None: ""}).fillna("").astype(str).str.strip()
    master.loc[master["design_key"].eq(""), "design_key"] = master["stem"]

    master["title_base"] = master["title_en"].fillna("").astype(str).str.strip()
    if settings.fi_title_fallback:
        fallback_mask = master["title_base"].eq("")
        master.loc[fallback_mask, "title_base"] = (
            master.loc[fallback_mask, "title_fi"].fillna("").astype(str).str.strip()
        )

    return master


def prepare_shopify_dataframe(raw: Optional[pd.DataFrame], mapping: Dict[str, Optional[str]]) -> pd.DataFrame:
    if raw is None:
        return pd.DataFrame(columns=["sku", "handle"])

    df = raw.copy()
    canonical = {
        "sku": df[mapping["sku"]].astype(str).str.strip() if mapping.get("sku") else "",
        "handle": df[mapping["handle"]].astype(str).str.strip() if mapping.get("handle") else "",
    }
    shopify = pd.DataFrame(canonical)
    shopify = shopify[shopify["sku"].astype(bool)]
    return shopify


def determine_handle(group: pd.DataFrame, sku_to_handle: Dict[str, str]) -> str:
    for sku in group["sku"]:
        handle = sku_to_handle.get(sku)
        if handle:
            return handle
    candidate = slugify(group["design_key"].iloc[0])
    if not candidate:
        candidate = slugify(group["stem"].iloc[0])
    return candidate or group["stem"].iloc[0]


def build_body_html(row: pd.Series) -> str:
    parts = [
        html_paragraph(row.get("title_fi", "")),
        html_paragraph(row.get("long_description_fi", "")),
        html_paragraph(row.get("short_name_en", "")),
    ]
    return "".join(part for part in parts if part)


def generate_transformation(
    master_raw: pd.DataFrame,
    shopify_raw: Optional[pd.DataFrame],
    master_mapping: Dict[str, Optional[str]],
    shopify_mapping: Dict[str, Optional[str]],
    settings: Settings,
) -> TransformationResult:
    master = prepare_master_dataframe(master_raw, master_mapping, settings)
    shopify = prepare_shopify_dataframe(shopify_raw, shopify_mapping)

    sku_to_handle = {row.sku: row.handle for row in shopify.itertuples(index=False) if row.handle}
    existing_skus = set(shopify["sku"].tolist())

    master_groups = list(master.groupby("design_key"))

    new_rows: List[Dict[str, str]] = []
    update_rows: List[Dict[str, str]] = []
    removed_rows: List[Dict[str, str]] = []
    needs_attention_rows: List[Dict[str, str]] = []

    report_new_products: List[Dict[str, str]] = []
    report_new_variants: List[Dict[str, str]] = []
    report_matched: List[Dict[str, str]] = []

    variant_constants = {
        "Variant Inventory Tracker": "Shopify",
        "Variant Inventory Policy": "continue",
        "Variant Fulfillment Service": "Manual",
    }

    preflight_notes: List[str] = []

    for design_key, group in master_groups:
        group = group.copy()
        group.sort_values(by=["stem", "suffix", "sku"], inplace=True)
        is_variant_product = group["has_hyphen"].all()
        handle = determine_handle(group, sku_to_handle)
        group_existing = group["sku"].isin(existing_skus)
        product_is_new = not group_existing.any()

        title_base = group["title_base"].iloc[0] if len(group) else ""
        stem = group["stem"].iloc[0] if len(group) else ""
        title = f"{title_base} - {stem}" if title_base else stem

        if product_is_new:
            first_row = True
            for _, row in group.iterrows():
                record: Dict[str, str] = {
                    "Command": "NEW" if first_row else "",
                    "Handle": handle,
                    "Variant SKU [ID]": row["sku"],
                    "Variant Price": format_decimal(row["retail_price"]),
                    "Variant Cost": format_decimal(row["wholesale_price"]),
                    "Variant Barcode": row.get("barcode", "") or "",
                    "Variant Grams": format_grams(
                        None if pd.isna(row.get("weight_kg")) else row.get("weight_kg") * 1000
                    ),
                }
                record.update(variant_constants)
                if is_variant_product:
                    record["Option1 Name"] = settings.option1_name
                    record["Option1 Value"] = row.get("suffix", "") or ""

                if first_row:
                    record.update(
                        {
                            "Title": title,
                            "Body HTML": build_body_html(row),
                            "Vendor": "Pandora",
                            "Status": "active",
                            "Published Scope": "global",
                            "Published": "FALSE",
                            "Tags": "",
                        }
                    )
                    first_row = False
                new_rows.append(record)

            report_new_products.append(
                {
                    "Design variation": design_key,
                    "Handle": handle,
                    "Total variants": len(group),
                }
            )
        else:
            for _, row in group.iterrows():
                sku = row["sku"]
                record: Dict[str, str] = {
                    "Command": "MERGE",
                    "Handle": handle,
                    "Variant SKU [ID]": sku,
                    "Variant Price": format_decimal(row["retail_price"]),
                    "Variant Cost": format_decimal(row["wholesale_price"]),
                    "Variant Barcode": row.get("barcode", "") or "",
                    "Variant Grams": format_grams(
                        None if pd.isna(row.get("weight_kg")) else row.get("weight_kg") * 1000
                    ),
                }
                if is_variant_product:
                    record["Option1 Name"] = settings.option1_name
                    record["Option1 Value"] = row.get("suffix", "") or ""

                if sku not in existing_skus:
                    record.update(variant_constants)
                    report_new_variants.append(
                        {
                            "SKU": sku,
                            "Handle": handle,
                            "Design variation": design_key,
                        }
                    )
                else:
                    report_matched.append(
                        {
                            "SKU": sku,
                            "Handle": handle,
                            "Design variation": design_key,
                        }
                    )

                update_rows.append(record)

        retail_invalid = group["retail_price"].isna() | (group["retail_price"] <= 0)
        wholesale_invalid = group["wholesale_price"].isna() | (group["wholesale_price"] <= 0)
        barcode_clean = clean_text_series(group["barcode"])
        barcode_numeric = pd.to_numeric(barcode_clean, errors="coerce")
        barcode_invalid = barcode_clean.eq("") | (barcode_numeric <= 0)

        attention_mask = retail_invalid | wholesale_invalid | barcode_invalid
        attention = group[attention_mask]
        for _, row in attention.iterrows():
            needs_attention_rows.append(
                {
                    "SKU": row["sku"],
                    "Design variation": design_key,
                    "Retail price": row["retail_price"],
                    "Wholesale price": row["wholesale_price"],
                    "Barcode": barcode_clean.loc[row.name],
                }
            )

    if existing_skus:
        master_skus = set(master["sku"].tolist())
        removed = sorted(existing_skus - master_skus)
        for sku in removed:
            removed_rows.append({"Command": "DELETE", "Variant SKU [ID]": sku})

    new_products_df = pd.DataFrame(new_rows)
    updates_df = pd.DataFrame(update_rows)
    removed_df = pd.DataFrame(removed_rows)
    attention_df = pd.DataFrame(needs_attention_rows)

    sync_report = {
        "New products": pd.DataFrame(report_new_products),
        "New variants": pd.DataFrame(report_new_variants),
        "Matched": pd.DataFrame(report_matched),
        "Removed": removed_df.copy(),
        "Needs attention": attention_df.copy(),
    }

    metrics = {
        "new_products": len(report_new_products),
        "new_variants": len(report_new_variants),
        "matched": len(report_matched),
        "removed": len(removed_rows),
        "needs_attention": len(needs_attention_rows),
    }

    if not new_rows:
        preflight_notes.append("No new products detected.")
    if not update_rows:
        preflight_notes.append("No rows generated for updates_safe.xlsx.")
    if existing_skus and not removed_rows:
        preflight_notes.append("No SKUs marked for removal.")

    return TransformationResult(
        new_products=new_products_df,
        updates_safe=updates_df,
        to_remove=removed_df,
        needs_attention=attention_df,
        sync_report=sync_report,
        metrics=metrics,
        preflight_notes=preflight_notes,
    )


# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------


def dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Sheet1") -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return buffer.getvalue()


def sync_report_to_excel_bytes(report: Dict[str, pd.DataFrame]) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        for sheet, frame in report.items():
            frame.to_excel(writer, index=False, sheet_name=sheet[:31] or "Sheet")
    return buffer.getvalue()


def build_zip_payload(files: Dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for filename, content in files.items():
            zf.writestr(filename, content)
    return buffer.getvalue()


def render_mapping_controls(title: str, columns: Sequence[str], auto_map: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
    st.subheader(title)
    selections: Dict[str, Optional[str]] = {}
    for logical, auto in auto_map.items():
        pretty_label = logical.replace("_", " ").title()
        options = ["(auto)"] + list(columns)
        default_index = 0
        if auto and auto in columns:
            default_index = options.index(auto)
        selection = st.selectbox(pretty_label, options, index=default_index, key=f"map_{title}_{logical}")
        selections[logical] = auto if selection == "(auto)" else selection
    return selections


def main() -> None:
    st.set_page_config(page_title="Pandora → Shopify (Matrixify) Sync", layout="wide")
    st.title("Pandora → Shopify (Matrixify) Sync")
    st.markdown(
        """
        Upload the latest Pandora master Excel file and, optionally, a Shopify export generated
        with Matrixify. Confirm the column mapping in the sidebar, adjust the settings if
        necessary, then generate Matrixify-ready workbooks for import.
        """
    )

    st.sidebar.header("Inputs")
    master_file = st.sidebar.file_uploader("Pandora master Excel", type=["xlsx"])
    shopify_file = st.sidebar.file_uploader("Shopify Matrixify export (optional)", type=["xlsx"])

    st.sidebar.header("Settings")
    option1_name = st.sidebar.text_input("Option1 name", value="Koko")
    fi_title_fallback = st.sidebar.checkbox("Use FI title as fallback", value=True)
    settings = Settings(option1_name=option1_name or "Koko", fi_title_fallback=fi_title_fallback)

    master_df: Optional[pd.DataFrame] = None
    shopify_df: Optional[pd.DataFrame] = None

    if master_file is not None:
        master_df = pd.read_excel(master_file)
        master_columns = list(master_df.columns)
        master_auto = auto_detect_mapping(master_columns, MASTER_ALIASES)
        st.sidebar.header("Master column mapping")
        master_selection = render_mapping_controls("master", master_columns, master_auto)
        master_mapping = reconcile_selection(master_selection, master_columns)
    else:
        master_mapping = {}

    if shopify_file is not None:
        shopify_df = pd.read_excel(shopify_file)
        shopify_columns = list(shopify_df.columns)
        shopify_auto = auto_detect_mapping(shopify_columns, SHOPIFY_ALIASES)
        st.sidebar.header("Shopify export mapping")
        shopify_selection = render_mapping_controls("shopify", shopify_columns, shopify_auto)
        shopify_mapping = reconcile_selection(shopify_selection, shopify_columns)
    else:
        shopify_mapping = {logical: None for logical in SHOPIFY_ALIASES.keys()}

    st.markdown("---")

    if master_df is None:
        st.info("Upload the Pandora master Excel file to begin.")
        return

    missing_master = ensure_required_fields(master_mapping, REQUIRED_MASTER_FIELDS)
    if missing_master:
        st.error(
            "Missing required master fields: " + ", ".join(field.replace("_", " ").title() for field in missing_master)
        )
        return

    if st.button("Generate Matrixify files", type="primary"):
        with st.spinner("Processing data…"):
            try:
                result = generate_transformation(
                    master_df,
                    shopify_df,
                    master_mapping,
                    shopify_mapping,
                    settings,
                )
            except Exception as exc:  # pragma: no cover - streamlit surface
                st.error(f"Failed to generate outputs: {exc}")
                return

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("New products", result.metrics["new_products"])
        col2.metric("New variants", result.metrics["new_variants"])
        col3.metric("Matched", result.metrics["matched"])
        col4.metric("Removed", result.metrics["removed"])
        col5.metric("Needs attention", result.metrics["needs_attention"])

        st.subheader("Downloads")
        files = {
            "new_products.xlsx": dataframe_to_excel_bytes(result.new_products, "New products"),
            "updates_safe.xlsx": dataframe_to_excel_bytes(result.updates_safe, "Updates"),
            "to_remove.xlsx": dataframe_to_excel_bytes(result.to_remove, "To remove"),
            "needs_attention.xlsx": dataframe_to_excel_bytes(result.needs_attention, "Needs attention"),
            "sync_report.xlsx": sync_report_to_excel_bytes(result.sync_report),
        }

        st.download_button(
            "Download new_products.xlsx",
            data=files["new_products.xlsx"],
            file_name="new_products.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        st.download_button(
            "Download updates_safe.xlsx",
            data=files["updates_safe.xlsx"],
            file_name="updates_safe.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        st.download_button(
            "Download to_remove.xlsx",
            data=files["to_remove.xlsx"],
            file_name="to_remove.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        st.download_button(
            "Download needs_attention.xlsx",
            data=files["needs_attention.xlsx"],
            file_name="needs_attention.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        st.download_button(
            "Download sync_report.xlsx",
            data=files["sync_report.xlsx"],
            file_name="sync_report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        zip_bytes = build_zip_payload(files)
        st.download_button(
            "Download all as ZIP",
            data=zip_bytes,
            file_name="pandora_matrixify_outputs.zip",
            mime="application/zip",
        )

        st.subheader("Preflight notes")
        if result.preflight_notes:
            for note in result.preflight_notes:
                st.write(f"• {note}")
        else:
            st.success("All preflight checks passed.")

        st.subheader("Sync report preview")
        for sheet, frame in result.sync_report.items():
            st.write(f"### {sheet}")
            if frame.empty:
                st.caption("No rows to display.")
            else:
                st.dataframe(frame)


if __name__ == "__main__":
    main()

