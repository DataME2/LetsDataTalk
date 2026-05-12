"""
Shared configuration for the North Star FC Lakeflow / Spark Declarative Pipeline.

Centralises paths, file patterns, schema definitions and reusable constants.
All bronze/silver/gold modules import from here so naming stays consistent.
"""

# ---------------------------------------------------------------------------
# Unity Catalog targets
# ---------------------------------------------------------------------------
CATALOG = "north_star_fc"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

# ---------------------------------------------------------------------------
# Source volume (where the upstream system drops snapshot CSVs)
# ---------------------------------------------------------------------------
SOURCE_VOLUME = "/Volumes/north_star_fc/nsfc/ext_files_ns"

# Glob patterns for each entity. We accept all snapshots dropped over time;
# silver reduces them to "latest snapshot per business key".
PATTERN_REGISTRATIONS = f"{SOURCE_VOLUME}/Registrations_*Season*.csv"
PATTERN_INVOICES      = f"{SOURCE_VOLUME}/RegistrationInvoices_*Season*.csv"
PATTERN_PLAYERS_DEBTS = f"{SOURCE_VOLUME}/PlayersDebts_*Season*.csv"
PATTERN_TEAM_DETAILS  = f"{SOURCE_VOLUME}/TeamDetails_*.csv"
PATTERN_TEAM_LISTS    = f"{SOURCE_VOLUME}/TeamLists_*.csv"

# Auto Loader checkpoint root (inside same volume keeps it self-contained)
CHECKPOINT_ROOT = f"{SOURCE_VOLUME}/_checkpoints"

# ---------------------------------------------------------------------------
# CSV reader options per entity
# ---------------------------------------------------------------------------
# Encodings replicate exactly what the Power Query M code declared per source.
# RegistrationInvoices, TeamDetails and PlayersDebts (2025/2026): Windows-1252
# PlayersDebts 2024 + TeamLists 2026: UTF-8 (65001)
# Registrations originally came from .xls; CSV exports are UTF-8.

CSV_OPTS_DEFAULT = {
    "header": "true",
    "multiLine": "true",
    "escape": "\"",
    "quote": "\"",
    "encoding": "windows-1252",
    "ignoreLeadingWhiteSpace": "true",
    "ignoreTrailingWhiteSpace": "true",
}

CSV_OPTS_UTF8 = {**CSV_OPTS_DEFAULT, "encoding": "UTF-8"}

# ---------------------------------------------------------------------------
# Business constants from the M code
# ---------------------------------------------------------------------------
# Postcode default when type-cast errors occur (M code: ReplaceErrorValues
# with 4000, the Brisbane CBD postcode — North Star FC plays in QLD).
DEFAULT_POSTCODE = 4000

# Fee category bucketing rules used for the invoice pivot. Order matters:
# the first rule that matches wins (mirrors the M code if/else chain).
FEE_CATEGORY_RULES = [
    ("Volunteer Levy",     ["volunteer levy", "reimbursement"]),
    ("Governing Body Fee", ["governing body"]),
    ("Sibling Discount",   ["sibling discount", "discount"]),
    ("Registration Fee",   ["player fee", "fq academy league", "junior metro",
                            "senior", "miniroos", "nsfc", "development",
                            "upgrade", "promotion", "additional fee",
                            "waitlist", "metro", "masters"]),
]
FEE_CATEGORY_DEFAULT = "Other Adjustments"

# Default value substitutions from M code (null / empty -> friendly text)
NULL_REPLACEMENTS_REGISTRATIONS = {
    "PrimaryMemberPhoneNumber":          "Check Mobile Number",
    "PrimaryMemberAlternateEmailAddress": "Check Primary Email Address",
    "PrimaryMemberSponsorInterest":      "No",
    "EmergencyContactEmailAddress":      "No email provided",
    "Player1PreviousClubName":           "No information provided",
    "Player1SchoolName":                 "No information School Provided",
    "Player1OtherSchool":                "Check Player School Name",
    "Player1SchoolGrade":                "No Provided",
    "Player1FQFeeBypassCode":            "No apply, ask registrar",
}

# Columns that get dropped from registrations because the M code did so
# (PII noise, free-text notes, duplicated identifiers).
COLUMNS_TO_DROP_REGISTRATIONS = [
    "RegistrationTime",
    "RegistrationChangesMade",
    "RegistrationChangeHistory",
    "NotifyEmailAddresses",
    "EmergencyContactNotes",
    "Player1Notes",
]

# The five player blocks in a single registration row are flattened in silver.
PLAYER_SLOTS = [1, 2, 3, 4, 5]

# Header columns that need forward-fill in invoices (after ghost rows are
# stripped) so each line item carries its parent invoice context.
INVOICE_HEADER_COLS = [
    "Surname", "First Name", "Reference", "Date",
    "Invoice Amount", "Paid Amount", "Outstanding Amount",
    "Commitment Amount", "Pending Amount", "Payable Amount",
    "Adjustment Indicator", "Component", "Component Description", "Event Type",
]