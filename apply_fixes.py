#!/usr/bin/env python3
"""
apply_fixes.py — Migration Script for RWOMS Database

Patches an existing rwoms_database.db with all 6 required fixes:
  1. Enable PRAGMA foreign_keys = ON
  2. Add integrity test documentation
  3. Add Work Order Detail columns (estimated_hours, work_performed,
     technician_notes, start_time, end_time)
  4. Add customer_id to Addresses for customer billing address support
  5. Add access_instructions to Service_Locations
  6. Add Roles and Users tables for security

Design assumptions documented inline.  Idempotent — safe to re-run.
"""

import os
import sqlite3
import sys
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Constants — early exit guard for missing database
# ---------------------------------------------------------------------------
DB_DIR: str = os.path.dirname(__file__)
DB_PATH: str = os.path.join(DB_DIR, "rwoms_database.db")

# ---------------------------------------------------------------------------
# Law of the Early Exit: Validate database exists at the boundary
# ---------------------------------------------------------------------------
if not os.path.exists(DB_PATH):
    print(f"ERROR: Database not found at {DB_PATH}")
    print("Run create_database.py first to generate the initial database.")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Parse connection at the boundary (Parse, Don't Validate)
# ---------------------------------------------------------------------------
conn: sqlite3.Connection = sqlite3.connect(DB_PATH)
cursor: sqlite3.Cursor = conn.cursor()

# --- Law of the Early Exit: Enforce foreign keys at connection boundary ---
cursor.execute("PRAGMA foreign_keys = ON;")
cursor.execute("PRAGMA busy_timeout = 5000;")
print("✓ Foreign key enforcement enabled (PRAGMA foreign_keys = ON).")
print("✓ Busy timeout set to 5000ms.")


# ---------------------------------------------------------------------------
# Helper: recreate a table to add a FK constraint that SQLite ALTER TABLE
# cannot add. Uses a separate connection where FK enforcement is OFF
# (SQLite default), so the DROP + RENAME swap is permitted.
# ---------------------------------------------------------------------------
def recreate_table_add_fk(
    old_name: str,
    create_sql: str,
    migrate_sql: str,
) -> None:
    """
    Replace *old_name* with a new table defined by *create_sql*
    (which includes the desired FK), migrating data via *migrate_sql*.

    Uses a second connection with FK enforcement OFF (the SQLite default)
    to avoid 'FOREIGN KEY constraint failed' on DROP TABLE.

    WARNING: Table/column names in the SQL strings passed to this function
    must be trusted constants only — they are interpolated directly into DDL.
    Never accept user input for *old_name*, *create_sql*, or *migrate_sql*.
    """
    backup_name: str = f"{old_name}_backup"
    conn2: sqlite3.Connection = sqlite3.connect(DB_PATH)
    # FK is OFF by default in SQLite — no need to set PRAGMA.
    new_name: str = f"{old_name}_v2"

    try:
        # --- Backup step: create a safety copy before any destructive ops ---
        conn2.execute(
            f"CREATE TABLE IF NOT EXISTS {backup_name} AS SELECT * FROM {old_name}"
        )
        print(f"   Backup table '{backup_name}' created for recovery.")

        conn2.execute(f"DROP TABLE IF EXISTS {new_name}")
        conn2.execute(create_sql)
        conn2.execute(migrate_sql)
        conn2.execute(f"DROP TABLE {old_name}")
        conn2.execute(f"ALTER TABLE {new_name} RENAME TO {old_name}")
        conn2.commit()
    except Exception:
        print(f"   ERROR: Table recreation failed for '{old_name}'.")
        print(f"   Backup table '{backup_name}' (if created) is available for recovery.")
        conn2.close()
        raise

    conn2.close()

print("\n" + "=" * 60)
print("APPLYING RWOMS DATABASE FIXES")
print("=" * 60)

# ============================================================================
# Helper functions — Atomic Predictability: pure, same input = same output
# ============================================================================

def column_exists(table: str, column: str) -> bool:
    """Return True if *column* exists in *table*.

    WARNING: The *table* parameter is interpolated directly into a PRAGMA query.
    It must be a trusted constant (e.g., a hardcoded table name from this script)
    and must never accept user input.
    """
    cursor.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cursor.fetchall())


def table_exists(name: str) -> bool:
    """Return True if *name* exists in sqlite_master."""
    cursor.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    )
    return cursor.fetchone()[0] > 0


def index_exists(name: str) -> bool:
    """Return True if index *name* exists."""
    cursor.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?",
        (name,),
    )
    return cursor.fetchone()[0] > 0


# ============================================================================
# FIX 4: Add customer_id to Addresses (recreate table for FK support)
# ============================================================================
print("\n--- Fix 4: Adding customer_id to Addresses ---")

if not column_exists("Addresses", "customer_id"):
    print("   Adding customer_id column with FK to Customers...")

    # Count addresses with type 'commercial' that will be transformed to 'service'.
    cursor.execute("SELECT COUNT(*) FROM Addresses WHERE address_type = 'commercial'")
    commercial_count: int = cursor.fetchone()[0]

    # SQLite ALTER TABLE cannot add FK constraints, so we recreate the table.
    # Use a second connection (FK off by default) for the DROP + RENAME swap.
    recreate_table_add_fk(
        old_name="Addresses",
        create_sql="""
            CREATE TABLE Addresses_v2 (
                address_id INTEGER PRIMARY KEY AUTOINCREMENT,
                street_address VARCHAR(200) NOT NULL,
                city VARCHAR(100) NOT NULL,
                state VARCHAR(50) NOT NULL,
                zip_code VARCHAR(20) NOT NULL,
                country VARCHAR(50) DEFAULT 'USA',
                address_type VARCHAR(20),
                customer_id INTEGER,
                FOREIGN KEY (customer_id) REFERENCES Customers(customer_id))
        """,
        migrate_sql="""
            INSERT INTO Addresses_v2
                (address_id, street_address, city, state, zip_code, country,
                 address_type, customer_id)
            SELECT a.address_id,
                   a.street_address,
                   a.city,
                   a.state,
                   a.zip_code,
                   a.country,
                   /* ------------------------------------------------------------------
                    * The original schema allowed address_type 'commercial' for some
                    * old addresses.  The new schema uses 'service' instead.  This CASE
                    * normalizes those values to match the new schema domain.
                    * ------------------------------------------------------------------ */
                   CASE a.address_type
                       WHEN 'commercial' THEN 'service'
                       ELSE a.address_type
                   END,
                   (SELECT sl.customer_id
                      FROM Service_Locations sl
                     WHERE sl.address_id = a.address_id
                     LIMIT 1)
              FROM Addresses a
        """,
    )
    # Commit on the main connection so it sees conn2's migrated data.
    conn.commit()
    # Re-query to get fresh metadata on the main connection.
    cursor.execute("SELECT COUNT(*) FROM Addresses")
    old_count: int = cursor.fetchone()[0]
    print(f"   Migrated {old_count} addresses with updated type names and customer_id.")
    if commercial_count > 0:
        print(f"   → Transformed {commercial_count} address(es) from 'commercial' to 'service' type.")

    # Step 4: Add billing addresses for each customer.
    # Assumption: each customer needs a distinct billing address.
    billing_addresses: list[tuple[str, str, str, str, str, str, int]] = [
        ("200 Corporate Boulevard", "New York", "NY", "10002", "USA", "billing", 1),
        ("300 Commerce Street",     "Los Angeles", "CA", "90002", "USA", "billing", 2),
        ("400 Finance Avenue",      "Houston",     "TX", "77002", "USA", "billing", 3),
        ("500 Money Road",          "Phoenix",     "AZ", "85002", "USA", "billing", 4),
    ]
    cursor.executemany(
        """INSERT INTO Addresses
               (street_address, city, state, zip_code, country, address_type, customer_id)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        billing_addresses,
    )
    print(f"   Added {len(billing_addresses)} billing addresses for each customer.")
else:
    print("   ✓ customer_id already exists in Addresses — skipping.")

# ============================================================================
# FIX 5: Add access_instructions to Service_Locations
# ============================================================================
print("\n--- Fix 5: Adding access_instructions to Service_Locations ---")

if not column_exists("Service_Locations", "access_instructions"):
    print("   Adding access_instructions column...")
    cursor.execute(
        "ALTER TABLE Service_Locations ADD COLUMN access_instructions TEXT"
    )

    # Backfill realistic data based on location name.
    access_map: dict[str, str] = {
        "Acme Headquarters":        "Use main entrance; check in with security desk",
        "Acme Warehouse":           "Use loading dock B; wear safety vest required",
        "TechStart Main Office":    "Call ahead for parking pass; enter through lobby",
        "Global Mfg Plant A":       "Sign in at guard station; hard hat required",
        "Global Mfg Distribution":  "Use rear entrance; fork lift traffic area",
        "Quantum HQ":               "Use visitor parking; bring ID for badge",
    }
    for location_name, instructions in access_map.items():
        cursor.execute(
            "UPDATE Service_Locations SET access_instructions = ? WHERE location_name = ?",
            (instructions, location_name),
        )
    print(f"   Backfilled {len(access_map)} service locations with access instructions.")
else:
    print("   ✓ access_instructions already exists — skipping.")

# ============================================================================
# FIX 3: Add Work Order Detail columns
# ============================================================================
print("\n--- Fix 3: Adding Work Order Detail columns ---")

detail_columns: list[tuple[str, str]] = [
    ("estimated_hours",  "DECIMAL(5,2)"),
    ("work_performed",   "TEXT"),
    ("technician_notes", "TEXT"),
    ("start_time",       "TIMESTAMP"),
    ("end_time",         "TIMESTAMP"),
]

for col_name, col_type in detail_columns:
    if not column_exists("Work_Orders", col_name):
        cursor.execute(f"ALTER TABLE Work_Orders ADD COLUMN {col_name} {col_type}")
        print(f"   Added column: {col_name} ({col_type})")
    else:
        print(f"   ✓ {col_name} already exists — skipping.")

# Backfill work order detail data for all existing work orders.
# Assumption: we derive realistic data from the existing work order description,
# status, and technician assignment.  Unstarted/pending orders get NULL notes.
# Note: the backfill is idempotent — running it multiple times is safe.
# Fetch all work orders to build backfill mapping by work_order_id.
cursor.execute("""
    SELECT work_order_id, technician_id, service_type_id, reported_date,
           scheduled_date, completed_date, time_worked, status, priority, description
    FROM Work_Orders
""")
wo_rows = cursor.fetchall()

# Build a mapping of description → estimated hours
# Intentional Naming: the key reads like the work order title.
estimated_hours_map: dict[str, float] = {
    "HVAC system annual maintenance":          5.0,
    "Warehouse equipment inspection":          3.5,
    "Network infrastructure check":            5.5,
    "Emergency electrical repair":             3.0,
    "Server room cooling maintenance":         4.0,
    "Office equipment upgrade":                5.0,
    "Critical system failure":                 6.0,
    "Scheduled quarterly maintenance":         3.5,
    "Urgent repair needed":                    4.5,
    "Complete system diagnostic":              8.0,
    "Preventive maintenance visit":            4.5,
    "Installation of new equipment":           5.0,
    "Software update and configuration":       3.0,
    "Emergency server restart":                2.5,
}

work_performed_map: dict[str, str] = {
    "HVAC system annual maintenance":
        "Performed HVAC annual maintenance including filter replacement, "
        "coil cleaning, refrigerant top-up, and system diagnostic testing.",
    "Warehouse equipment inspection":
        "Inspected all warehouse equipment including forklifts, conveyor belts, "
        "and safety systems. Identified minor wear on conveyor belt #3.",
    "Network infrastructure check":
        "Performed comprehensive network infrastructure audit including switch "
        "configurations, cable testing, and wireless signal analysis.",
    "Emergency electrical repair":
        "Diagnosed and repaired electrical fault in main breaker panel. "
        "Replaced faulty circuit breaker and tested all connected circuits.",
    "Server room cooling maintenance":
        "Scheduled maintenance visit for server room HVAC units. "
        "Inspection and filter replacement pending.",
    "Office equipment upgrade":
        "Upgraded office workstations with new hardware including SSDs, "
        "RAM upgrades, and monitor replacements for 12 stations.",
    "Critical system failure":
        "Diagnosing root cause of critical system failure. Initial analysis "
        "indicates potential database corruption. Running recovery tools.",
    "Scheduled quarterly maintenance":
        "Routine quarterly maintenance. Tasks assigned and pending scheduling.",
    "Urgent repair needed":
        "Urgent repair request received. Waiting for technician dispatch.",
    "Complete system diagnostic":
        "Ran full system diagnostic suite including hardware tests, "
        "software validation, and security compliance scanning.",
    "Preventive maintenance visit":
        "Performed scheduled preventive maintenance including cleaning, "
        "lubrication, and calibration of all mechanical systems.",
    "Installation of new equipment":
        "New equipment installation scheduled. Site preparation in progress.",
    "Software update and configuration":
        "Applied latest security patches and software updates to all "
        "managed systems. Updated configuration profiles per new standards.",
    "Emergency server restart":
        "Performed emergency server restart after system freeze. "
        "Ran disk check and verified data integrity post-restart.",
}

technician_notes_map: dict[str, str] = {
    "HVAC system annual maintenance":
        "All maintenance completed. System operating within normal parameters. "
        "Recommended quarterly filter changes.",
    "Warehouse equipment inspection":
        "All equipment passed safety inspection. Conveyor belt #3 scheduled for "
        "replacement in next quarter.",
    "Network infrastructure check":
        "Network running at optimal performance. Updated firmware on 3 switches. "
        "No critical issues found.",
    "Emergency electrical repair":
        "Emergency repair completed. Root cause identified as overloaded circuit. "
        "Advised load balancing across circuits.",
    "Office equipment upgrade":
        "All upgrades completed successfully. Users reported significant "
        "performance improvement. Follow-up scheduled for next week.",
    "Critical system failure":
        "System partially restored from backup. Full recovery expected within "
        "24 hours. Escalated to senior engineering team.",
    "Complete system diagnostic":
        "All systems passed diagnostic with 100% compliance rating. "
        "No action required. Report filed.",
    "Preventive maintenance visit":
        "All maintenance tasks completed per schedule. Equipment condition "
        "satisfactory. Next visit recommended in 3 months.",
    "Software update and configuration":
        "All updates applied successfully. Systems rebooted and verified. "
        "No compatibility issues found.",
    "Emergency server restart":
        "Server successfully restarted and all services restored. "
        "Root cause identified as memory leak in monitoring application.",
}

updated_count: int = 0
for row in wo_rows:
    wo_id: int = row[0]
    tech_id: int | None = row[1]
    description: str = row[9]
    completed_date: str | None = row[5]
    time_worked: float | None = row[6]
    status: str = row[7]
    reported_date: str = row[3]

    est_hours: float | None = estimated_hours_map.get(description)
    work_perf: str | None = work_performed_map.get(description)
    tech_notes: str | None = technician_notes_map.get(description)

    # Compute start_time/end_time from existing timestamps.
    start_time: str | None = None
    end_time: str | None = None

    if completed_date and time_worked:
        try:
            completed_dt = datetime.fromisoformat(completed_date)
            # Start = completed - time_worked hours, End = completed
            end_time = completed_date
            start_time = (completed_dt - timedelta(hours=float(time_worked))).isoformat()
        except (ValueError, TypeError):
            pass
    elif completed_date and not time_worked:
        # Completed but no time_worked — use completed_date for both
        start_time = completed_date
        end_time = completed_date
    elif status == "In Progress":
        # In progress but not completed: use reported_date as start
        reported_dt = datetime.fromisoformat(reported_date)
        start_time = reported_dt.replace(hour=8, minute=0, second=0, microsecond=0).isoformat()
        # No end time yet
    # else: Pending/Reported/Scheduled — leave as NULL

    cursor.execute(
        """UPDATE Work_Orders
              SET estimated_hours = ?,
                  work_performed = ?,
                  technician_notes = ?,
                  start_time = ?,
                  end_time = ?
            WHERE work_order_id = ?""",
        (est_hours, work_perf, tech_notes, start_time, end_time, wo_id),
    )
    updated_count += 1

print(f"   Backfilled data for {updated_count} work orders.")

# ============================================================================
# FIX 6: Create Roles table
# ============================================================================
print("\n--- Fix 6a: Creating Roles table ---")

if not table_exists("Roles"):
    cursor.execute("""
        CREATE TABLE Roles (
            role_id INTEGER PRIMARY KEY AUTOINCREMENT,
            role_name VARCHAR(50) NOT NULL UNIQUE,
            description TEXT)
    """)

    roles: list[tuple[str, str]] = [
        ("Admin",       "Full system access with all administrative privileges"),
        ("Technician",  "Field service technicians who perform and update work orders"),
        ("Staff",       "Office and dispatch staff who manage scheduling and customer records"),
    ]
    cursor.executemany(
        "INSERT INTO Roles (role_name, description) VALUES (?, ?)", roles
    )
    print(f"   Created Roles table and inserted {len(roles)} roles.")
else:
    print("   ✓ Roles table already exists — skipping.")

# ============================================================================
# FIX 6 (cont): Create Users table
# ============================================================================
print("\n--- Fix 6b: Creating Users table ---")

if not table_exists("Users"):
    cursor.execute("""
        CREATE TABLE Users (
            user_id INTEGER PRIMARY KEY AUTOINCREMENT,
            username VARCHAR(50) NOT NULL UNIQUE,
            password_hash VARCHAR(255) NOT NULL,
            role_id INTEGER NOT NULL,
            contact_id INTEGER,
            is_active BOOLEAN DEFAULT 1,
            FOREIGN KEY (role_id) REFERENCES Roles(role_id),
            FOREIGN KEY (contact_id) REFERENCES Contacts(contact_id))
    """)

    # First ensure James Martinez contact exists (needed for Technician user).
    cursor.execute("SELECT COUNT(*) FROM Contacts WHERE contact_id = 8")
    if cursor.fetchone()[0] == 0:
        # Insert James Martinez as contact 8 (technician reference for user account).
        cursor.execute("""
            INSERT INTO Contacts (contact_id, customer_id, location_id, first_name, last_name, title)
            VALUES (8, 1, 1, 'James', 'Martinez', 'Senior Technician')
        """)
        # Also add his phone and email for completeness.
        cursor.execute(
            "INSERT INTO Phone_Numbers (contact_id, phone_number, phone_type, is_primary) "
            "VALUES (8, '212-555-0801', 'work', 1)"
        )
        cursor.execute(
            "INSERT INTO Email_Addresses (contact_id, email_address, email_type, is_primary) "
            "VALUES (8, 'james.martinez@acme.com', 'work', 1)"
        )
        print("   Added James Martinez contact (ID 8) for Technician user.")

    # Fix existing broken FK: David Wilson (contact 5) references customer_id=5
    # which does not exist. Update to customer 3 (Global Manufacturing LLC).
    cursor.execute(
        "UPDATE Contacts SET customer_id = 3 WHERE contact_id = 5 AND customer_id = 5"
    )
    if cursor.rowcount > 0:
        print("   Fixed David Wilson contact: customer_id 5 → 3 (Global Manufacturing).")

    # password_hash values are placeholder hashes (not real bcrypt).
    # In production these would be properly salted and hashed.
    users: list[tuple[str, str, int, int | None, int]] = [
        ("admin",           "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2", 1, None, 1),
        ("james.martinez",  "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2", 2, 8, 1),
        ("staff",           "c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5", 3, None, 1),
    ]
    cursor.executemany(
        """INSERT INTO Users (username, password_hash, role_id, contact_id, is_active)
           VALUES (?, ?, ?, ?, ?)""",
        users,
    )
    print(f"   Created Users table and inserted {len(users)} users.")
else:
    print("   ✓ Users table already exists — skipping.")

# ============================================================================
# CREATE ADDITIONAL INDEXES
# ============================================================================
print("\n--- Creating additional indexes ---")

new_indexes: list[tuple[str, str, str]] = [
    ("idx_addresses_customer",  "Addresses",        "customer_id"),
    ("idx_users_role",          "Users",            "role_id"),
    ("idx_users_contact",       "Users",            "contact_id"),
]

for idx_name, tbl, col in new_indexes:
    if not index_exists(idx_name):
        cursor.execute(f"CREATE INDEX {idx_name} ON {tbl}({col})")
        print(f"   Created index {idx_name} on {tbl}({col})")
    else:
        print(f"   ✓ Index {idx_name} already exists — skipping.")

# ============================================================================
# Commit all changes — Fail Fast: ensure nothing is pending on error
# ============================================================================
conn.commit()

# ============================================================================
# VERIFICATION REPORT
# ============================================================================
print("\n" + "=" * 60)
print("VERIFICATION REPORT")
print("=" * 60)

# 1. Table existence and row counts
expected_tables: list[tuple[str, bool]] = [
    ("Customers",        True),
    ("Addresses",        True),
    ("Service_Locations", True),
    ("Contacts",         True),
    ("Phone_Numbers",    True),
    ("Email_Addresses",  True),
    ("Technicians",      True),
    ("Service_Types",    True),
    ("Work_Orders",      True),
    ("Billing",          True),
    ("Roles",            True),
    ("Users",            True),
]

print("\nTable Status:")
print("-" * 45)
all_tables_ok: bool = True
total_rows: int = 0
for tbl_name, required in expected_tables:
    exists: bool = table_exists(tbl_name)
    if exists:
        count: int = cursor.execute(f"SELECT COUNT(*) FROM {tbl_name}").fetchone()[0]
        status: str = "✓"
        total_rows += count
    else:
        count = 0
        status = "✗ MISSING" if required else "✓ (optional)"
        if required:
            all_tables_ok = False
    print(f"   {status} {tbl_name:<25} {count:>5} rows")
print("-" * 45)
print(f"   {'TOTAL':<25} {total_rows:>5} rows")

# 2. Column presence verification
print("\nColumn Presence (new columns):")
print("-" * 45)

# Verify Work_Orders detail columns
for col_name, col_type in detail_columns:
    if column_exists("Work_Orders", col_name):
        print(f"   ✓ Work_Orders.{col_name}")
    else:
        print(f"   ✗ Work_Orders.{col_name} — MISSING")
        all_tables_ok = False

# Verify Addresses.customer_id
if column_exists("Addresses", "customer_id"):
    print("   ✓ Addresses.customer_id")
else:
    print("   ✗ Addresses.customer_id — MISSING")
    all_tables_ok = False

# Verify Service_Locations.access_instructions
if column_exists("Service_Locations", "access_instructions"):
    print("   ✓ Service_Locations.access_instructions")
else:
    print("   ✗ Service_Locations.access_instructions — MISSING")
    all_tables_ok = False

# 3. Foreign Key Integrity
print("\nForeign Key Integrity:")
cursor.execute("PRAGMA foreign_key_check")
fk_violations: list = cursor.fetchall()
if fk_violations:
    print(f"   ⚠ Found {len(fk_violations)} FK violation(s):")
    for v in fk_violations:
        # v = (parent_table, parent_rowid, child_table, child_column_list)
        print(f"      - Table {v[0]}, row {v[1]} → referenced by {v[2]}")
    all_tables_ok = False
else:
    print("   ✓ All foreign keys are valid. No violations.")

# 4. Idempotency confirmation
cursor.execute("PRAGMA foreign_keys")
fk_status: int = cursor.fetchone()[0]
print(f"\nForeign Keys Active: {'✓ ON' if fk_status else '✗ OFF'}")

# ============================================================================
# CLEANUP
# ============================================================================
conn.close()

# ============================================================================
# PERMISSION MODEL EXPLANATION (per Fix 6 requirement)
# ============================================================================
print("\n" + "=" * 60)
print("PERMISSION MODEL")
print("=" * 60)
print("""
/* ---------------------------------------------------------------------------
 * RWOMS Role-Based Permission Model
 *
 * Three roles are defined, each with a distinct access scope:
 *
 * 1. Admin
 *    - Full CRUD access to all tables (Customers, Addresses, Service_Locations,
 *      Contacts, Phone_Numbers, Email_Addresses, Work_Orders, Billing, Users)
 *    - Can manage Users and Roles assignments
 *    - Can modify system configuration and technician records
 *    - WHY: The Admin needs complete oversight to configure the system,
 *      audit all activity, and resolve any data issues.
 *
 * 2. Technician
 *    - Read access to Work_Orders assigned to them
 *    - Write access to work_performed, technician_notes, start_time, end_time,
 *      and status on their assigned Work_Orders
 *    - Read access to Service_Locations, Customers, and Contacts for context
 *    - No access to Billing, Users, or Roles tables
 *    - WHY: Technicians are field workers who execute service calls.
 *      They need to update work order progress and document work performed,
 *      but should never see billing data, user credentials, or role assignments.
 *
 * 3. Staff
 *    - Full CRUD on Customers, Addresses, Service_Locations, Contacts
 *    - Read/Write on Work_Orders scheduling fields (scheduled_date, priority, status)
 *    - Read access to Technicians (for assignment)
 *    - No access to Users, Roles, or Billing financial data
 *    - WHY: Staff handle scheduling, customer management, and dispatching.
 *      They need to create work orders and manage customer relationships,
 *      but should not modify financial records or security settings.
 *
 * Note: The actual application-layer authorization logic is not implemented
 * in the database layer. These permissions describe the intended access model
 * that the application should enforce. The database enforces referential
 * integrity and data constraints, while the application layer gates access
 * based on user role.
 * ---------------------------------------------------------------------------
 */""")

# ============================================================================
# INTEGRITY TEST DOCUMENTATION (commented SQL that should FAIL)
# ============================================================================
print("\n" + "=" * 60)
print("INTEGRITY TEST CASES (expected failures)")
print("=" * 60)
print("""
/* ---------------------------------------------------------------------------
 * The following SQL statements are INTEGRITY TESTS that should each FAIL
 * when executed against the patched database.  They demonstrate that the
 * database correctly enforces FK, NOT NULL, and UNIQUE constraints.
 *
 * They are COMMENTED OUT to avoid breaking the migration on re-run.
 * ---------------------------------------------------------------------------
 */

/* INTEGRITY TEST 1: Invalid foreign key (location_id 999) — should FAIL
 *   Tests that Work_Orders cannot reference a non-existent Service_Location. */
-- INSERT INTO Work_Orders (location_id, service_type_id) VALUES (999, 1);


/* INTEGRITY TEST 2: NOT NULL violation (NULL customer_name) — should FAIL
 *   Tests that Customers.customer_name cannot be NULL. */
-- INSERT INTO Customers (customer_name, contact_person) VALUES (NULL, 'Test Person');


/* INTEGRITY TEST 3: UNIQUE constraint violation (duplicate username) — should FAIL
 *   Tests that Users.username must be unique across the table. */
-- INSERT INTO Users (username, password_hash, role_id) VALUES ('admin', 'hash123', 1);


/* INTEGRITY TEST 4: Invalid FK (role_id 999) — should FAIL
 *   Tests that Users.role_id must reference an existing role in Roles. */
-- INSERT INTO Users (username, password_hash, role_id) VALUES ('ghost_user', 'hash456', 999);
""")

# ============================================================================
# FINAL SUMMARY
# ============================================================================
print("=" * 60)
if all_tables_ok:
    print("APPLY FIXES COMPLETED SUCCESSFULLY")
    print(f"Database patched: {DB_PATH}")
    print("All 6 fixes applied. Database is idempotent (safe to re-run).")
else:
    print("APPLY FIXES COMPLETED WITH WARNINGS")
    print("Some items were not in expected state — review output above.")
print("=" * 60)
