def mapper(rec: dict) -> dict:
    return {
        "NPI": rec.get("NPI", ""),
        "First Name": rec.get("First Name", ""),
        "Last Name": rec.get("Last Name", ""),

        "primaryTelephone": rec.get("Primary Telephone", ""),
        "state": rec.get("State", ""),
        "stateLicense": rec.get("State License", ""),
        "suffix": rec.get("Suffix", ""),

        "surescriptsAddressLine1": rec.get("Surescripts Address Line1", ""),
        "surescriptsAddressLine2": rec.get("Surescripts Address Line2", ""),
        "surescriptsCity": rec.get("Surescripts City", ""),
        "surescriptsCountryCode": rec.get("Surescripts Country Code", ""),
        "surescriptsFax": rec.get("Surescripts Fax", ""),
        "surescriptsPermissions": rec.get("Surescripts Permissions", ""),
        "surescriptsPostalCode": rec.get("Surescripts Postal Code", ""),
        "surescriptsPrimaryTelephone": rec.get("Surescripts Primary Telephone", ""),
        "surescriptsSPI": rec.get("Surescripts SPI", ""),
        "surescriptsState": rec.get("Surescripts State", ""),
    }

mapped_records = []

for rec in records:
    mapped_records.append(mapper(rec))

print("✅ Total mapped records:", len(mapped_records))
print(mapped_records[0])
