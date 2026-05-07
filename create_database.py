#!/usr/bin/env python3
"""
Create Metropolitan Retail Company RWOMS SQLite Database
Creates all 12 tables based on 3NF schema with sample data
Includes: foreign key enforcement, work order details, customer billing addresses,
          access instructions, and role-based security (Users & Roles)
"""

import os
import random
import sqlite3
from datetime import datetime, timedelta

# Seed random for reproducible billing data
random.seed(42)

# ---------------------------------------------------------------------------
# Database path
# ---------------------------------------------------------------------------
DB_PATH = os.path.join(os.path.dirname(__file__), "rwoms_database.db")

# Remove existing database if present
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)
    print(f"Removed existing database: {DB_PATH}")

# Connect to database (creates new file)
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# --- Law of the Early Exit: Enforce foreign keys at connection boundary ---
cursor.execute("PRAGMA foreign_keys = ON;")
print("Foreign key enforcement enabled.")

print("\n" + "=" * 60)
print("CREATING RWOMS DATABASE")
print("=" * 60)

# ============================================================================
# TABLE 1: Customers
# ============================================================================
print("\n1. Creating Customers table...")
cursor.execute("""
CREATE TABLE Customers (
    customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_name VARCHAR(100) NOT NULL,
    contact_person VARCHAR(100),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
""")

# Insert sample customers
customers = [
    ("Acme Corporation", "John Smith"),
    ("TechStart Industries", "Sarah Johnson"),
    ("Global Manufacturing LLC", "Michael Brown"),
    ("Quantum Electronics", "Emily Davis"),
]
cursor.executemany(
    "INSERT INTO Customers (customer_name, contact_person) VALUES (?, ?)", customers
)
print(f"   - Inserted {len(customers)} customers")

# ============================================================================
# TABLE 2: Addresses  (now with customer_id FK for billing/service addresses)
# ============================================================================
print("2. Creating Addresses table...")
cursor.execute("""
CREATE TABLE Addresses (
    address_id INTEGER PRIMARY KEY AUTOINCREMENT,
    street_address VARCHAR(200) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(50) NOT NULL,
    zip_code VARCHAR(20) NOT NULL,
    country VARCHAR(50) DEFAULT 'USA',
    address_type VARCHAR(20),
    customer_id INTEGER,
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id))
""")

# Insert sample addresses
# Each existing service address is assigned to the owning customer.
# New billing addresses give each customer a distinct billing location.
addresses = [
    # (street, city, state, zip, country, type, customer_id)
    ("123 Main Street", "New York", "NY", "10001", "USA", "service", 1),
    ("456 Oak Avenue", "Los Angeles", "CA", "90001", "USA", "service", 2),
    ("789 Pine Road", "Chicago", "IL", "60601", "USA", "warehouse", 1),
    ("321 Elm Boulevard", "Houston", "TX", "77001", "USA", "service", 3),
    ("555 Cedar Lane", "Phoenix", "AZ", "85001", "USA", "service", 4),
    ("100 Industrial Way", "Seattle", "WA", "98101", "USA", "warehouse", 3),
    # --- New billing addresses ---
    ("200 Corporate Boulevard", "New York", "NY", "10002", "USA", "billing", 1),
    ("300 Commerce Street", "Los Angeles", "CA", "90002", "USA", "billing", 2),
    ("400 Finance Avenue", "Houston", "TX", "77002", "USA", "billing", 3),
    ("500 Money Road", "Phoenix", "AZ", "85002", "USA", "billing", 4),
]
cursor.executemany(
    """INSERT INTO Addresses (street_address, city, state, zip_code, country, address_type, customer_id)
       VALUES (?, ?, ?, ?, ?, ?, ?)""",
    addresses,
)
print(f"   - Inserted {len(addresses)} addresses")

# ============================================================================
# TABLE 3: Service_Locations  (now with access_instructions)
# ============================================================================
print("3. Creating Service_Locations table...")
cursor.execute("""
CREATE TABLE Service_Locations (
    location_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    location_name VARCHAR(100) NOT NULL,
    address_id INTEGER NOT NULL,
    is_active BOOLEAN DEFAULT 1,
    access_instructions TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id),
    FOREIGN KEY (address_id) REFERENCES Addresses(address_id))
""")

# Insert service locations with realistic access instructions
service_locations = [
    # (customer_id, location_name, address_id, is_active, access_instructions)
    (1, "Acme Headquarters", 1, 1, "Use main entrance; check in with security desk"),
    (1, "Acme Warehouse", 3, 1, "Use loading dock B; wear safety vest required"),
    (2, "TechStart Main Office", 2, 1, "Call ahead for parking pass; enter through lobby"),
    (3, "Global Mfg Plant A", 4, 1, "Sign in at guard station; hard hat required"),
    (3, "Global Mfg Distribution", 6, 1, "Use rear entrance; fork lift traffic area"),
    (4, "Quantum HQ", 5, 1, "Use visitor parking; bring ID for badge"),
]
cursor.executemany(
    """INSERT INTO Service_Locations (customer_id, location_name, address_id, is_active, access_instructions)
       VALUES (?, ?, ?, ?, ?)""",
    service_locations,
)
print(f"   - Inserted {len(service_locations)} service locations")

# ============================================================================
# TABLE 4: Contacts
# ============================================================================
print("4. Creating Contacts table...")
cursor.execute("""
CREATE TABLE Contacts (
    contact_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER,
    location_id INTEGER,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    title VARCHAR(100),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id),
    FOREIGN KEY (location_id) REFERENCES Service_Locations(location_id))
""")

# Insert contacts (include a contact for James Martinez used by Users table)
contacts = [
    (1, 1, "John", "Smith", "CEO"),
    (2, 2, "Sarah", "Johnson", "Operations Manager"),
    (3, 3, "Michael", "Brown", "Facility Director"),
    (4, 4, "Emily", "Davis", "IT Manager"),
    (3, 4, "David", "Wilson", "Plant Manager"),  # customer 3 (Global Mfg), location 4 (Plant A)
    (1, 6, "Jennifer", "Taylor", "Warehouse Supervisor"),
    (2, None, "Robert", "Anderson", "Sales Representative"),
    # New contact for technician user account
    (1, 1, "James", "Martinez", "Senior Technician"),
]
cursor.executemany(
    """INSERT INTO Contacts (customer_id, location_id, first_name, last_name, title)
       VALUES (?, ?, ?, ?, ?)""",
    contacts,
)
print(f"   - Inserted {len(contacts)} contacts")

# ============================================================================
# TABLE 5: Phone_Numbers
# ============================================================================
print("5. Creating Phone_Numbers table...")
cursor.execute("""
CREATE TABLE Phone_Numbers (
    phone_id INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_id INTEGER NOT NULL,
    phone_number VARCHAR(20) NOT NULL,
    phone_type VARCHAR(20),
    is_primary BOOLEAN DEFAULT 0,
    FOREIGN KEY (contact_id) REFERENCES Contacts(contact_id))
""")

# Insert phone numbers (multiple per contact)
phone_numbers = [
    (1, "212-555-0101", "work", 1),
    (1, "212-555-0102", "mobile", 0),
    (2, "213-555-0201", "work", 1),
    (2, "213-555-0202", "mobile", 0),
    (2, "213-555-0203", "fax", 0),
    (3, "312-555-0301", "work", 1),
    (4, "713-555-0401", "work", 1),
    (4, "713-555-0402", "mobile", 0),
    (5, "602-555-0501", "work", 1),
    (6, "206-555-0601", "work", 1),
    (6, "206-555-0602", "mobile", 0),
    (7, "312-555-0701", "mobile", 1),
    (7, "312-555-0702", "work", 0),
    # Phone for James Martinez (contact 8)
    (8, "212-555-0801", "work", 1),
]
cursor.executemany(
    """INSERT INTO Phone_Numbers (contact_id, phone_number, phone_type, is_primary)
       VALUES (?, ?, ?, ?)""",
    phone_numbers,
)
print(f"   - Inserted {len(phone_numbers)} phone numbers")

# ============================================================================
# TABLE 6: Email_Addresses
# ============================================================================
print("6. Creating Email_Addresses table...")
cursor.execute("""
CREATE TABLE Email_Addresses (
    email_id INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_id INTEGER NOT NULL,
    email_address VARCHAR(100) NOT NULL,
    email_type VARCHAR(20),
    is_primary BOOLEAN DEFAULT 0,
    FOREIGN KEY (contact_id) REFERENCES Contacts(contact_id))
""")

# Insert email addresses (multiple per contact)
email_addresses = [
    (1, "john.smith@acme.com", "work", 1),
    (1, "john.smith.personal@gmail.com", "personal", 0),
    (2, "sarah.j@techstart.com", "work", 1),
    (2, "sarah.johnson@email.com", "personal", 0),
    (3, "mbrown@globalmfg.com", "work", 1),
    (4, "emily.davis@quantum.com", "work", 1),
    (4, "emily.davis.quantum@gmail.com", "alternate", 0),
    (5, "dwilson@globalmfg.com", "work", 1),
    (6, "jtaylor@acme.com", "work", 1),
    (7, "randerson@techstart.com", "work", 1),
    # Email for James Martinez (contact 8)
    (8, "james.martinez@acme.com", "work", 1),
]
cursor.executemany(
    """INSERT INTO Email_Addresses (contact_id, email_address, email_type, is_primary)
       VALUES (?, ?, ?, ?)""",
    email_addresses,
)
print(f"   - Inserted {len(email_addresses)} email addresses")

# ============================================================================
# TABLE 7: Technicians
# ============================================================================
print("7. Creating Technicians table...")
cursor.execute("""
CREATE TABLE Technicians (
    technician_id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    employee_id VARCHAR(20) UNIQUE,
    hourly_rate DECIMAL(8,2) NOT NULL,
    hire_date DATE,
    is_active BOOLEAN DEFAULT 1,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
""")

# Insert technicians
base_date = datetime(2023, 1, 1).date()
technicians = [
    ("James", "Martinez", "TECH001", 45.00, base_date, 1),
    ("Lisa", "Garcia", "TECH002", 55.00, base_date + timedelta(days=30), 1),
    ("Thomas", "Lee", "TECH003", 40.00, base_date + timedelta(days=60), 1),
    ("Amanda", "White", "TECH004", 60.00, base_date + timedelta(days=90), 1),
    ("Christopher", "Kim", "TECH005", 50.00, base_date + timedelta(days=120), 1),
]
cursor.executemany(
    """INSERT INTO Technicians (first_name, last_name, employee_id, hourly_rate, hire_date, is_active)
       VALUES (?, ?, ?, ?, ?, ?)""",
    technicians,
)
print(f"   - Inserted {len(technicians)} technicians")

# ============================================================================
# TABLE 8: Service_Types
# ============================================================================
print("8. Creating Service_Types table...")
cursor.execute("""
CREATE TABLE Service_Types (
    service_type_id INTEGER PRIMARY KEY AUTOINCREMENT,
    service_name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    base_fee DECIMAL(8,2) DEFAULT 0.00)
""")

# Insert service types
service_types = [
    ("Preventive Maintenance", "Regular scheduled maintenance to prevent issues", 150.00),
    ("Emergency Repair", "Urgent repair for critical issues", 200.00),
    ("System Installation", "New equipment or system installation", 500.00),
    ("Inspection & Testing", "Comprehensive inspection and testing services", 100.00),
    ("Equipment Upgrade", "Upgrade existing equipment or systems", 350.00),
    ("Diagnostic Analysis", "Technical diagnostic and troubleshooting", 125.00),
]
cursor.executemany(
    """INSERT INTO Service_Types (service_name, description, base_fee)
       VALUES (?, ?, ?)""",
    service_types,
)
print(f"   - Inserted {len(service_types)} service types")

# ============================================================================
# TABLE 9: Work_Orders  (now with estimated_hours, work_performed,
#                         technician_notes, start_time, end_time)
# ============================================================================
print("9. Creating Work_Orders table...")
cursor.execute("""
CREATE TABLE Work_Orders (
    work_order_id INTEGER PRIMARY KEY AUTOINCREMENT,
    location_id INTEGER NOT NULL,
    technician_id INTEGER,
    service_type_id INTEGER NOT NULL,
    reported_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    scheduled_date DATE,
    completed_date TIMESTAMP,
    time_worked DECIMAL(5,2),
    status VARCHAR(20),
    priority VARCHAR(10),
    description TEXT,
    estimated_hours DECIMAL(5,2),
    work_performed TEXT,
    technician_notes TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (location_id) REFERENCES Service_Locations(location_id),
    FOREIGN KEY (technician_id) REFERENCES Technicians(technician_id),
    FOREIGN KEY (service_type_id) REFERENCES Service_Types(service_type_id))
""")


# Helper: return a datetime offset from now
def days_ago(days):
    return datetime.now() - timedelta(days=days)


# Helper: build a timestamp at 08:00 on a given day
def at_8am(dt):
    return dt.replace(hour=8, minute=0, second=0, microsecond=0)


# Helper: add hours to a timestamp for end_time calculation
def add_hours(dt, hours):
    return dt + timedelta(hours=hours)


# Work orders with all detail fields
# Columns: location_id, technician_id, service_type_id, reported_date, scheduled_date,
#          completed_date, time_worked, status, priority, description,
#          estimated_hours, work_performed, technician_notes, start_time, end_time
work_orders = [
    (
        1, 1, 1,
        days_ago(10), days_ago(5).date(), days_ago(5), 4.5,
        "Completed", "High", "HVAC system annual maintenance",
        5.0,
        "Performed HVAC annual maintenance including filter replacement, "
        "coil cleaning, refrigerant top-up, and system diagnostic testing.",
        "All maintenance completed. System operating within normal parameters. "
        "Recommended quarterly filter changes.",
        at_8am(days_ago(5)),
        add_hours(at_8am(days_ago(5)), 4.5),
    ),
    (
        1, 2, 2,
        days_ago(8), days_ago(3).date(), days_ago(3), 3.0,
        "Completed", "Medium", "Warehouse equipment inspection",
        3.5,
        "Inspected all warehouse equipment including forklifts, conveyor belts, "
        "and safety systems. Identified minor wear on conveyor belt #3.",
        "All equipment passed safety inspection. Conveyor belt #3 scheduled for "
        "replacement in next quarter.",
        at_8am(days_ago(3)),
        add_hours(at_8am(days_ago(3)), 3.0),
    ),
    (
        2, 3, 1,
        days_ago(7), days_ago(2).date(), days_ago(2), 6.0,
        "Completed", "Low", "Network infrastructure check",
        5.5,
        "Performed comprehensive network infrastructure audit including switch "
        "configurations, cable testing, and wireless signal analysis.",
        "Network running at optimal performance. Updated firmware on 3 switches. "
        "No critical issues found.",
        at_8am(days_ago(2)),
        add_hours(at_8am(days_ago(2)), 6.0),
    ),
    (
        3, 1, 4,
        days_ago(5), days_ago(1).date(), days_ago(1), 2.5,
        "Completed", "High", "Emergency electrical repair",
        3.0,
        "Diagnosed and repaired electrical fault in main breaker panel. "
        "Replaced faulty circuit breaker and tested all connected circuits.",
        "Emergency repair completed. Root cause identified as overloaded circuit. "
        "Advised load balancing across circuits.",
        at_8am(days_ago(1)),
        add_hours(at_8am(days_ago(1)), 2.5),
    ),
    (
        3, None, 2,
        days_ago(3), None, None, None,
        "Scheduled", "Medium", "Server room cooling maintenance",
        4.0,
        None,
        None,
        None,
        None,
    ),
    (
        4, 3, 3,
        days_ago(2), days_ago(0).date(), days_ago(0), 5.0,
        "Completed", "Low", "Office equipment upgrade",
        5.0,
        "Upgraded office workstations with new hardware including SSDs, "
        "RAM upgrades, and monitor replacements for 12 stations.",
        "All upgrades completed successfully. Users reported significant "
        "performance improvement. Follow-up scheduled for next week.",
        at_8am(days_ago(0)),
        add_hours(at_8am(days_ago(0)), 5.0),
    ),
    (
        4, 4, 5,
        days_ago(1), None, None, None,
        "In Progress", "High", "Critical system failure",
        6.0,
        "Diagnosing root cause of critical system failure. Initial analysis "
        "indicates potential database corruption. Running recovery tools.",
        "System partially restored from backup. Full recovery expected within "
        "24 hours. Escalated to senior engineering team.",
        at_8am(days_ago(1)),
        None,
    ),
    (
        5, 2, 1,
        days_ago(0), None, None, None,
        "Pending", "Medium", "Scheduled quarterly maintenance",
        3.5,
        None,
        None,
        None,
        None,
    ),
    (
        6, None, 2,
        days_ago(0), None, None, None,
        "Reported", "High", "Urgent repair needed",
        4.5,
        None,
        None,
        None,
        None,
    ),
    (
        1, 5, 4,
        days_ago(4), days_ago(1).date(), days_ago(1), 8.0,
        "Completed", "Low", "Complete system diagnostic",
        8.0,
        "Ran full system diagnostic suite including hardware tests, "
        "software validation, and security compliance scanning.",
        "All systems passed diagnostic with 100% compliance rating. "
        "No action required. Report filed.",
        at_8am(days_ago(1)),
        add_hours(at_8am(days_ago(1)), 8.0),
    ),
    (
        2, 1, 3,
        days_ago(6), days_ago(4).date(), days_ago(4), 4.0,
        "Completed", "Medium", "Preventive maintenance visit",
        4.5,
        "Performed scheduled preventive maintenance including cleaning, "
        "lubrication, and calibration of all mechanical systems.",
        "All maintenance tasks completed per schedule. Equipment condition "
        "satisfactory. Next visit recommended in 3 months.",
        at_8am(days_ago(4)),
        add_hours(at_8am(days_ago(4)), 4.0),
    ),
    (
        5, 2, 5,
        days_ago(0), None, None, None,
        "Scheduled", "Medium", "Installation of new equipment",
        5.0,
        None,
        None,
        None,
        None,
    ),
    (
        1, 4, 6,
        days_ago(9), days_ago(4).date(), days_ago(4), 3.5,
        "Completed", "Medium", "Software update and configuration",
        3.0,
        "Applied latest security patches and software updates to all "
        "managed systems. Updated configuration profiles per new standards.",
        "All updates applied successfully. Systems rebooted and verified. "
        "No compatibility issues found.",
        at_8am(days_ago(4)),
        add_hours(at_8am(days_ago(4)), 3.5),
    ),
    (
        2, 5, 2,
        days_ago(5), days_ago(2).date(), days_ago(2), 2.0,
        "Completed", "Low", "Emergency server restart",
        2.5,
        "Performed emergency server restart after system freeze. "
        "Ran disk check and verified data integrity post-restart.",
        "Server successfully restarted and all services restored. "
        "Root cause identified as memory leak in monitoring application.",
        at_8am(days_ago(2)),
        add_hours(at_8am(days_ago(2)), 2.0),
    ),
]
cursor.executemany(
    """INSERT INTO Work_Orders
       (location_id, technician_id, service_type_id, reported_date, scheduled_date,
        completed_date, time_worked, status, priority, description,
        estimated_hours, work_performed, technician_notes, start_time, end_time)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
    work_orders,
)
print(f"   - Inserted {len(work_orders)} work orders")

# ============================================================================
# TABLE 10: Billing
# ============================================================================
print("10. Creating Billing table...")
cursor.execute("""
CREATE TABLE Billing (
    billing_id INTEGER PRIMARY KEY AUTOINCREMENT,
    work_order_id INTEGER NOT NULL UNIQUE,
    labor_cost DECIMAL(10,2),
    service_fee DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    billing_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    due_date DATE,
    paid_date TIMESTAMP,
    payment_status VARCHAR(20),
    FOREIGN KEY (work_order_id) REFERENCES Work_Orders(work_order_id))
""")

# Calculate billing data: labor_cost = hourly_rate * time_worked
# service_fee = service_type base_fee
# total_amount = labor_cost + service_fee

# First get the technician hourly rates and service type fees
cursor.execute("SELECT technician_id, hourly_rate FROM Technicians")
tech_rates = {row[0]: row[1] for row in cursor.fetchall()}

cursor.execute("SELECT service_type_id, base_fee FROM Service_Types")
service_fees = {row[0]: row[1] for row in cursor.fetchall()}

# Get work orders with their technician and service type
cursor.execute("""
    SELECT wo.work_order_id, wo.technician_id, wo.service_type_id, wo.time_worked, wo.status
    FROM Work_Orders wo
    WHERE wo.time_worked IS NOT NULL
""")

billing_records = []
for wo_id, tech_id, svc_type_id, time_worked, status in cursor.fetchall():
    if tech_id and time_worked:
        # Get hourly rate for this technician
        hourly_rate = tech_rates.get(tech_id, 0)
        labor_cost = round(hourly_rate * float(time_worked), 2)

        # Get service fee
        service_fee = float(service_fees.get(svc_type_id, 0))

        # Calculate total
        total = round(labor_cost + service_fee, 2)

        # Determine due date (30 days from now)
        due_date = (datetime.now() + timedelta(days=30)).date()

        # Determine payment status based on work order status
        if status == "Completed":
            payment_status = random.choice(["Paid", "Pending", "Pending", "Pending"])
            paid_date = (
                (datetime.now() - timedelta(days=random.randint(1, 20)))
                if payment_status == "Paid"
                else None
            )
        else:
            payment_status = "Unbilled"
            paid_date = None

        billing_records.append(
            (wo_id, labor_cost, service_fee, total, due_date, paid_date, payment_status)
        )

cursor.executemany(
    """INSERT INTO Billing
       (work_order_id, labor_cost, service_fee, total_amount, due_date, paid_date, payment_status)
       VALUES (?, ?, ?, ?, ?, ?, ?)""",
    billing_records,
)
print(f"   - Inserted {len(billing_records)} billing records")

# ============================================================================
# TABLE 11: Roles
# ============================================================================
print("11. Creating Roles table...")
cursor.execute("""
CREATE TABLE Roles (
    role_id INTEGER PRIMARY KEY AUTOINCREMENT,
    role_name VARCHAR(50) NOT NULL UNIQUE,
    description TEXT)
""")

roles = [
    ("Admin", "Full system access with all administrative privileges"),
    ("Technician", "Field service technicians who perform and update work orders"),
    ("Staff", "Office and dispatch staff who manage scheduling and customer records"),
]
cursor.executemany(
    "INSERT INTO Roles (role_name, description) VALUES (?, ?)", roles
)
print(f"   - Inserted {len(roles)} roles")

# ============================================================================
# TABLE 12: Users
# ============================================================================
print("12. Creating Users table...")
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

# password_hash values are placeholder hashes (not real bcrypt).
# In production these would be properly salted and hashed.
users = [
    ("admin", "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2", 1, None, 1),
    ("james.martinez", "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2", 2, 8, 1),
    ("staff", "c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5", 3, None, 1),
]
cursor.executemany(
    """INSERT INTO Users (username, password_hash, role_id, contact_id, is_active)
       VALUES (?, ?, ?, ?, ?)""",
    users,
)
print(f"   - Inserted {len(users)} users")

# ============================================================================
# CREATE INDEXES
# ============================================================================
print("\n" + "=" * 60)
print("CREATING INDEXES")
print("=" * 60)

indexes = [
    ("idx_work_orders_location", "Work_Orders", "location_id"),
    ("idx_work_orders_technician", "Work_Orders", "technician_id"),
    ("idx_work_orders_status", "Work_Orders", "status"),
    ("idx_billing_status", "Billing", "payment_status"),
    ("idx_contacts_customer", "Contacts", "customer_id"),
    ("idx_service_locations_customer", "Service_Locations", "customer_id"),
    # New indexes for FK query performance
    ("idx_addresses_customer", "Addresses", "customer_id"),
    ("idx_users_role", "Users", "role_id"),
    ("idx_users_contact", "Users", "contact_id"),
]

# NOTE: index_name, table, and column below are interpolated directly into DDL.
# They are trusted constants from the hardcoded list above and must never
# accept user input.
for index_name, table, column in indexes:
    cursor.execute(f"CREATE INDEX {index_name} ON {table}({column})")
    print(f"   - Created index {index_name}")

# Commit all changes
conn.commit()

# ============================================================================
# VERIFY DATABASE
# ============================================================================
print("\n" + "=" * 60)
print("DATABASE VERIFICATION")
print("=" * 60)

# Get table count
cursor.execute(
    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
)
table_count = cursor.fetchone()[0]
print(f"\nTotal Tables: {table_count}")

# Get row counts for each table
tables = [
    "Customers",
    "Addresses",
    "Service_Locations",
    "Contacts",
    "Phone_Numbers",
    "Email_Addresses",
    "Technicians",
    "Service_Types",
    "Work_Orders",
    "Billing",
    "Roles",
    "Users",
]

print("\nRow Counts by Table:")
print("-" * 40)
total_rows = 0
# NOTE: *table* below is interpolated directly into a query string.  It is a
# trusted constant from the hardcoded list above and must never accept user input.
for table in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    print(f"   {table:<25} {count:>5}")
    total_rows += count

print("-" * 40)
print(f"   {'TOTAL':<25} {total_rows:>5}")

# Verify foreign key integrity
print("\nForeign Key Integrity Check:")
cursor.execute("PRAGMA foreign_key_check")
fk_issues = cursor.fetchall()
if fk_issues:
    print("   WARNING: Foreign key violations found!")
    for issue in fk_issues:
        print(f"   - {issue}")
else:
    print("   All foreign keys are valid. No violations.")

# Close connection
conn.close()

print("\n" + "=" * 60)
print(f"DATABASE CREATED SUCCESSFULLY: {DB_PATH}")
print("=" * 60)

# ============================================================================
# INTEGRITY TEST CASES (commented — these should FAIL when run)
# ============================================================================
#
# Test 1: Invalid foreign key — location_id 999 does not exist
# /* INTEGRITY TEST: Invalid FK (location_id 999) — should fail */
# INSERT INTO Work_Orders (location_id, service_type_id)
# VALUES (999, 1);
#
# Test 2: NOT NULL constraint — customer_name cannot be NULL
# /* INTEGRITY TEST: NOT NULL violation (NULL customer_name) — should fail */
# INSERT INTO Customers (customer_name, contact_person)
# VALUES (NULL, 'Test Person');
#
# Test 3: UNIQUE constraint — duplicate username
# /* INTEGRITY TEST: UNIQUE violation (duplicate username) — should fail */
# INSERT INTO Users (username, password_hash, role_id)
# VALUES ('admin', 'hash123', 1);
#
# Test 4: FK constraint — Users.role_id must reference an existing role
# /* INTEGRITY TEST: Invalid FK (role_id 999 does not exist in Roles) — should fail */
# INSERT INTO Users (username, password_hash, role_id)
# VALUES ('ghost_user', 'hash456', 999);
