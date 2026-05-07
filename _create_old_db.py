#!/usr/bin/env python3
"""
Generate an OLD-STYLE RWOMS database for testing the migration script.
This creates the database as it existed BEFORE any fixes were applied.
"""
import os
import random
import sqlite3
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), "rwoms_database.db")

if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# NOTE: No PRAGMA foreign_keys = ON (old behavior)

# Table 1: Customers
cursor.execute("""
CREATE TABLE Customers (
    customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_name VARCHAR(100) NOT NULL,
    contact_person VARCHAR(100),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
""")

customers = [
    ("Acme Corporation", "John Smith"),
    ("TechStart Industries", "Sarah Johnson"),
    ("Global Manufacturing LLC", "Michael Brown"),
    ("Quantum Electronics", "Emily Davis"),
]
cursor.executemany(
    "INSERT INTO Customers (customer_name, contact_person) VALUES (?, ?)", customers
)

# Table 2: Addresses (OLD schema — no customer_id)
cursor.execute("""
CREATE TABLE Addresses (
    address_id INTEGER PRIMARY KEY AUTOINCREMENT,
    street_address VARCHAR(200) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(50) NOT NULL,
    zip_code VARCHAR(20) NOT NULL,
    country VARCHAR(50) DEFAULT 'USA',
    address_type VARCHAR(20))
""")

addresses = [
    ("123 Main Street", "New York", "NY", "10001", "USA", "commercial"),
    ("456 Oak Avenue", "Los Angeles", "CA", "90001", "USA", "commercial"),
    ("789 Pine Road", "Chicago", "IL", "60601", "USA", "warehouse"),
    ("321 Elm Boulevard", "Houston", "TX", "77001", "USA", "commercial"),
    ("555 Cedar Lane", "Phoenix", "AZ", "85001", "USA", "service"),
    ("100 Industrial Way", "Seattle", "WA", "98101", "USA", "warehouse"),
]
cursor.executemany(
    """INSERT INTO Addresses (street_address, city, state, zip_code, country, address_type)
       VALUES (?, ?, ?, ?, ?, ?)""",
    addresses,
)

# Table 3: Service_Locations (OLD schema — no access_instructions)
cursor.execute("""
CREATE TABLE Service_Locations (
    location_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    location_name VARCHAR(100) NOT NULL,
    address_id INTEGER NOT NULL,
    is_active BOOLEAN DEFAULT 1,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id),
    FOREIGN KEY (address_id) REFERENCES Addresses(address_id))
""")

service_locations = [
    (1, "Acme Headquarters", 1, 1),
    (1, "Acme Warehouse", 3, 1),
    (2, "TechStart Main Office", 2, 1),
    (3, "Global Mfg Plant A", 4, 1),
    (3, "Global Mfg Distribution", 6, 1),
    (4, "Quantum HQ", 5, 1),
]
cursor.executemany(
    """INSERT INTO Service_Locations (customer_id, location_name, address_id, is_active)
       VALUES (?, ?, ?, ?)""",
    service_locations,
)

# Table 4: Contacts
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

contacts = [
    (1, 1, "John", "Smith", "CEO"),
    (2, 2, "Sarah", "Johnson", "Operations Manager"),
    (3, 3, "Michael", "Brown", "Facility Director"),
    (4, 4, "Emily", "Davis", "IT Manager"),
    (5, 5, "David", "Wilson", "Plant Manager"),
    (1, 6, "Jennifer", "Taylor", "Warehouse Supervisor"),
    (2, None, "Robert", "Anderson", "Sales Representative"),
]
cursor.executemany(
    """INSERT INTO Contacts (customer_id, location_id, first_name, last_name, title)
       VALUES (?, ?, ?, ?, ?)""",
    contacts,
)

# Table 5: Phone_Numbers
cursor.execute("""
CREATE TABLE Phone_Numbers (
    phone_id INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_id INTEGER NOT NULL,
    phone_number VARCHAR(20) NOT NULL,
    phone_type VARCHAR(20),
    is_primary BOOLEAN DEFAULT 0,
    FOREIGN KEY (contact_id) REFERENCES Contacts(contact_id))
""")

phone_numbers = [
    (1, "212-555-0101", "work", 1),
    (1, "212-555-0102", "mobile", 0),
    (2, "213-555-0201", "work", 1),
    (2, "213-555-0202", "mobile", 0),
    (2, "213-555-0203", "fax", 0),
    (3, "312-555-0301", "work", 1),
    (4, "713-555-0401", "work", 1),
    (4, "713-555-0402", "mobile", 1),
    (5, "602-555-0501", "work", 1),
    (6, "206-555-0601", "work", 1),
    (6, "206-555-0602", "mobile", 0),
    (7, "312-555-0701", "mobile", 1),
    (7, "312-555-0702", "work", 0),
]
cursor.executemany(
    """INSERT INTO Phone_Numbers (contact_id, phone_number, phone_type, is_primary)
       VALUES (?, ?, ?, ?)""",
    phone_numbers,
)

# Table 6: Email_Addresses
cursor.execute("""
CREATE TABLE Email_Addresses (
    email_id INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_id INTEGER NOT NULL,
    email_address VARCHAR(100) NOT NULL,
    email_type VARCHAR(20),
    is_primary BOOLEAN DEFAULT 0,
    FOREIGN KEY (contact_id) REFERENCES Contacts(contact_id))
""")

email_addresses = [
    (1, "john.smith@acme.com", "work", 1),
    (1, "john.smith.personal@gmail.com", "personal", 0),
    (2, "sarah.j@techstart.com", "work", 1),
    (2, "sarah.johnson@email.com", "personal", 0),
    (3, "mbrown@globalmfg.com", "work", 1),
    (4, "emily.davis@quantum.com", "work", 1),
    (4, "emily.davis.quantum@gmail.com", "alternate", 1),
    (5, "dwilson@globalmfg.com", "work", 1),
    (6, "jtaylor@acme.com", "work", 1),
    (7, "randerson@techstart.com", "work", 1),
]
cursor.executemany(
    """INSERT INTO Email_Addresses (contact_id, email_address, email_type, is_primary)
       VALUES (?, ?, ?, ?)""",
    email_addresses,
)

# Table 7: Technicians
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

# Table 8: Service_Types
cursor.execute("""
CREATE TABLE Service_Types (
    service_type_id INTEGER PRIMARY KEY AUTOINCREMENT,
    service_name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    base_fee DECIMAL(8,2) DEFAULT 0.00)
""")

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

# Table 9: Work_Orders (OLD schema — no detail columns)
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
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (location_id) REFERENCES Service_Locations(location_id),
    FOREIGN KEY (technician_id) REFERENCES Technicians(technician_id),
    FOREIGN KEY (service_type_id) REFERENCES Service_Types(service_type_id))
""")

def days_ago(days):
    return datetime.now() - timedelta(days=days)

work_orders = [
    (1, 1, 1, days_ago(10), days_ago(5), days_ago(5), 4.5, "Completed", "High", "HVAC system annual maintenance"),
    (1, 2, 2, days_ago(8), days_ago(3), days_ago(3), 3.0, "Completed", "Medium", "Warehouse equipment inspection"),
    (2, 3, 1, days_ago(7), days_ago(2), days_ago(2), 6.0, "Completed", "Low", "Network infrastructure check"),
    (3, 1, 4, days_ago(5), days_ago(1), days_ago(1), 2.5, "Completed", "High", "Emergency electrical repair"),
    (3, None, 2, days_ago(3), None, None, None, "Scheduled", "Medium", "Server room cooling maintenance"),
    (4, 3, 3, days_ago(2), days_ago(0), days_ago(0), 5.0, "Completed", "Low", "Office equipment upgrade"),
    (4, 4, 5, days_ago(1), None, None, None, "In Progress", "High", "Critical system failure"),
    (5, 2, 1, days_ago(0), None, None, None, "Pending", "Medium", "Scheduled quarterly maintenance"),
    (6, None, 2, days_ago(0), None, None, None, "Reported", "High", "Urgent repair needed"),
    (1, 5, 4, days_ago(4), days_ago(1), days_ago(1), 8.0, "Completed", "Low", "Complete system diagnostic"),
    (2, 1, 3, days_ago(6), days_ago(4), days_ago(4), 4.0, "Completed", "Medium", "Preventive maintenance visit"),
    (5, 2, 5, days_ago(0), None, None, None, "Scheduled", "Medium", "Installation of new equipment"),
    (1, 4, 6, days_ago(9), days_ago(4), days_ago(4), 3.5, "Completed", "Medium", "Software update and configuration"),
    (2, 5, 2, days_ago(5), days_ago(2), days_ago(2), 2.0, "Completed", "Low", "Emergency server restart"),
]
cursor.executemany(
    """INSERT INTO Work_Orders
       (location_id, technician_id, service_type_id, reported_date, scheduled_date,
        completed_date, time_worked, status, priority, description)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
    work_orders,
)

# Table 10: Billing
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

cursor.execute("SELECT technician_id, hourly_rate FROM Technicians")
tech_rates = {row[0]: row[1] for row in cursor.fetchall()}
cursor.execute("SELECT service_type_id, base_fee FROM Service_Types")
service_fees = {row[0]: row[1] for row in cursor.fetchall()}

cursor.execute("""
    SELECT wo.work_order_id, wo.technician_id, wo.service_type_id, wo.time_worked, wo.status
    FROM Work_Orders wo WHERE wo.time_worked IS NOT NULL
""")

billing_records = []
for wo_id, tech_id, svc_type_id, time_worked, status in cursor.fetchall():
    if tech_id and time_worked:
        hourly_rate = tech_rates.get(tech_id, 0)
        labor_cost = round(hourly_rate * float(time_worked), 2)
        service_fee = float(service_fees.get(svc_type_id, 0))
        total = round(labor_cost + service_fee, 2)
        due_date = (datetime.now() + timedelta(days=30)).date()
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

# Indexes (original set only)
indexes = [
    ("idx_work_orders_location", "Work_Orders", "location_id"),
    ("idx_work_orders_technician", "Work_Orders", "technician_id"),
    ("idx_work_orders_status", "Work_Orders", "status"),
    ("idx_billing_status", "Billing", "payment_status"),
    ("idx_contacts_customer", "Contacts", "customer_id"),
    ("idx_service_locations_customer", "Service_Locations", "customer_id"),
]
for index_name, table, column in indexes:
    cursor.execute(f"CREATE INDEX {index_name} ON {table}({column})")

conn.commit()
conn.close()

print(f"Old-style database created: {DB_PATH}")
print("10 tables, no Roles/Users, no detail columns, no customer_id in Addresses.")
