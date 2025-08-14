# %%
# Load and Clean Dirty Email Addresses
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from build.clean_emails.email_primitives import emails

# %%
# Initialize Spark Session
spark = (
    SparkSession.builder.appName("EmailDataCleaning")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

print(f"Spark Version: {spark.version}")


# %%
# Create a DataFrame with test email addresses for email primitives
test_emails_data = [
    # Invalid formats
    (1, "missing_at_symbol.com", "Missing @ symbol", "invalid"),
    (2, "@no_username.com", "No username before @", "invalid"),
    (3, "no_domain@", "No domain after @", "invalid"),
    (
        4,
        "   spaces in email@example.com   ",
        "Contains spaces and needs trimming",
        "fixable",
    ),
    (5, "double@@symbol.com", "Multiple @ symbols", "invalid"),
    (6, "user@domain@extra.com", "Multiple @ symbols", "invalid"),
    # Common typos that can be fixed
    (7, "john.doe@gmai.com", "Gmail typo - missing 'l'", "typo"),
    (8, "jane.smith@gmial.com", "Gmail typo - transposed letters", "typo"),
    (9, "bob@yahooo.com", "Yahoo typo - extra 'o'", "typo"),
    (10, "alice@hotmial.com", "Hotmail typo", "typo"),
    (11, "user@outlok.com", "Outlook typo - missing 'o'", "typo"),
    (12, "person@company.cmo", "TLD typo - .cmo instead of .com", "typo"),
    (13, "admin@business.ent", "TLD typo - .ent instead of .net", "typo"),
    (14, "contact@organization.rog", "TLD typo - .rog instead of .org", "typo"),
    # Gmail specific cases
    (15, "john.smith@gmail.com", "Gmail with dots", "gmail"),
    (16, "j.o.h.n.s.m.i.t.h@gmail.com", "Gmail with many dots", "gmail"),
    (17, "johnsmith+newsletter@gmail.com", "Gmail with plus addressing", "gmail"),
    (
        18,
        "john.smith+work+urgent@googlemail.com",
        "Googlemail with dots and plus",
        "gmail",
    ),
    # Plus addressing in other providers
    (19, "user+tag@outlook.com", "Outlook with plus addressing", "plus"),
    (20, "customer+2024@yahoo.com", "Yahoo with plus addressing", "plus"),
    # Corporate vs free email providers
    (21, "employee@company.com", "Corporate email", "corporate"),
    (22, "contact@business.org", "Corporate email", "corporate"),
    (23, "user@gmail.com", "Free provider - Gmail", "free"),
    (24, "person@yahoo.com", "Free provider - Yahoo", "free"),
    (25, "someone@hotmail.com", "Free provider - Hotmail", "free"),
    (26, "user@icloud.com", "Free provider - iCloud", "free"),
    (27, "person@protonmail.com", "Free provider - ProtonMail", "free"),
    # Disposable email addresses
    (28, "temp@10minutemail.com", "Disposable email service", "disposable"),
]

test_emails_df = spark.createDataFrame(
    test_emails_data, ["id", "email", "issue_description", "category"]
)

print("Test Email Addresses DataFrame:")
print(f"Total test cases: {test_emails_df.count()}")
test_emails_df.show(60, truncate=False)

# %%
# Import email primitives and test various functions


# Test email validation
print("\n=== Testing Email Validation ===")
validation_df = test_emails_df.select(
    "id",
    "email",
    "category",
    emails.is_valid_email(F.col("email")).alias("is_valid"),
    emails.is_valid_username(F.col("email")).alias("valid_username"),
    emails.is_valid_domain(F.col("email")).alias("valid_domain"),
)
validation_df.show(10, truncate=False)

# %%
# Test email extraction functions
print("\n=== Testing Email Extraction ===")
extraction_df = test_emails_df.select(
    "id",
    "email",
    emails.extract_username(F.col("email")).alias("username"),
    emails.extract_domain(F.col("email")).alias("domain"),
    emails.extract_domain_name(F.col("email")).alias("domain_name"),
    emails.extract_tld(F.col("email")).alias("tld"),
)
extraction_df.filter(F.col("email").isNotNull()).show(10, truncate=False)

# %%
# Test typo fixing
print("\n=== Testing Typo Fixes ===")
typo_df = test_emails_df.filter(F.col("category") == "typo").select(
    "id",
    "email",
    "issue_description",
    emails.fix_common_typos(F.col("email")).alias("fixed_email"),
    emails.is_valid_email(emails.fix_common_typos(F.col("email"))).alias(
        "valid_after_fix"
    ),
)
typo_df.show(truncate=False)

# %%
# Test Gmail-specific functions
print("\n=== Testing Gmail Functions ===")
gmail_df = test_emails_df.filter(F.col("category") == "gmail").select(
    "id",
    "email",
    "issue_description",
    emails.has_plus_addressing(F.col("email")).alias("has_plus"),
    emails.remove_dots_from_gmail(F.col("email")).alias("no_dots"),
    emails.remove_plus_addressing(F.col("email")).alias("no_plus"),
    emails.normalize_gmail(F.col("email")).alias("normalized"),
)
# gmail_df.show(truncate=False)

# %%
# Test corporate vs free email detection
print("\n=== Testing Email Provider Detection ===")
provider_df = test_emails_df.filter(
    F.col("category").isin(["corporate", "free", "disposable"])
).select(
    "id",
    "email",
    "category",
    emails.is_corporate_email(F.col("email")).alias("is_corporate"),
    emails.is_disposable_email(F.col("email")).alias("is_disposable"),
    emails.get_email_provider(F.col("email")).alias("provider"),
)
provider_df.show(truncate=False)

# %%
# Test standardization on all emails
print("\n=== Testing Email Standardization ===")
standardized_df = test_emails_df.select(
    "id",
    "email",
    "category",
    emails.remove_whitespace(F.col("email")).alias("no_whitespace"),
    emails.lowercase_email(F.col("email")).alias("lowercase"),
    # emails.standardize_email(F.col("email")).alias("standardized"),
    # emails.get_canonical_email(F.col("email")).alias("canonical"),
)
standardized_df.filter(
    F.col("category").isin(["fixable", "valid", "gmail", "typo"])
).show(15, truncate=False)

# %%
# Test name extraction and masking
print("\n=== Testing Name Extraction and Masking ===")
name_df = test_emails_df.filter(
    F.col("email").isNotNull() & F.col("email").contains("@")
).select(
    "id",
    "email",
    emails.extract_name_from_email(F.col("email")).alias("extracted_name"),
    emails.mask_email(F.col("email")).alias("masked_email"),
)
name_df.filter(F.col("extracted_name") != "").show(10, truncate=False)

# %%
# Summary statistics
print("\n=== Summary Statistics ===")
valid_count = test_emails_df.filter(emails.is_valid_email(F.col("email"))).count()
print(f"Valid emails: {valid_count}")

# fixable_count = test_emails_df.filter(
#    ~emails.is_valid_email(F.col("email"))
#    & emails.is_valid_email(emails.standardize_email(F.col("email")))
# ).count()

# print(f"Total emails: {test_emails_df.count()}")
# print(f"Fixable emails: {fixable_count}")
# print(f"Invalid emails: {test_emails_df.count() - valid_count - fixable_count}")

# %%
