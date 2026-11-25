"""
090 — PII Encryption, Salted Hashing & ANSI-Safe AES Decryption (aes_encrypt, try_aes_decrypt).

Scenario & Scale:
    A payment ledger (`1B+` card transactions) must comply with PCI-DSS by
    encrypting Primary Account Numbers (PANs) with AES-GCM, producing salted
    SHA-256 join tokens, and dropping cleartext columns before Bronze/Silver writes.
"""

from __future__ import annotations

import os


# ==============================================================================
# 2. Anti-Pattern: Hardcoded Keys, Unsalted Hashes & Unsafe aes_decrypt
# ==============================================================================
# ❌ Bad Practice:
#   df.withColumn("enc", F.expr("aes_encrypt(pan, '1234567890123456')"))
#   df.withColumn("hash", F.sha2("pan", 256))
#   df.withColumn("dec", F.expr("cast(aes_decrypt(enc, '1234567890123456') AS STRING)"))
#
# Otherwise:
#   Hardcoded keys in code/Git compromise the entire ledger. Unsalted SHA-256 on
#   16-digit PANs is trivially reversed via rainbow tables. Under Spark 4.0 ANSI
#   mode, `aes_decrypt` aborts the whole job if a single row has corrupt ciphertext.


# ==============================================================================
# 3. ANSI-Safe PySpark 4.0+ Fix: Parameterized Secrets + GCM + try_aes_decrypt
# ==============================================================================
# ✅ Good Practice: Inject keys from a secret provider via parameterized SQL args,
#                   use AES-GCM + salted SHA-256, and decrypt with `try_aes_decrypt`.
def resolve_crypto_material(scope: str = "pci_scope") -> tuple[str, str]:
    """Fetch 16/24/32-byte AES key and salt from env/secret manager (never hardcode prod keys)."""
    aes_key = os.environ.get(f"{scope.upper()}_AES_KEY", "dev_only_16b_key")
    salt = os.environ.get(f"{scope.upper()}_PAN_SALT", "dev_only_salt_token")
    if len(aes_key.encode("utf-8")) not in (16, 24, 32):
        raise ValueError("AES key must be 16, 24, or 32 bytes.")
    return aes_key, salt


def encrypt_and_tokenize_pans(df, aes_key: str, salt: str):
    """Encrypt PANs with AES-GCM, compute salted SHA-256 token, and drop cleartext."""
    from pyspark.sql import functions as F

    return (
        df.withColumn("encrypted_pan", F.aes_encrypt(F.col("card_number"), F.lit(aes_key).cast("binary"), F.lit("GCM")))
        .withColumn("pan_token", F.sha2(F.concat(F.col("card_number"), F.lit(salt)), 256))
        .drop("card_number")
    )


def decrypt_pans_ansi_safe(df, aes_key: str):
    """Use Spark 4.0+ `try_aes_decrypt` so corrupted ciphertext returns NULL instead of failing."""
    from pyspark.sql import functions as F

    return df.withColumn(
        "decrypted_pan",
        F.expr("CAST(try_aes_decrypt(encrypted_pan, CAST(:aes_key AS BINARY), 'GCM') AS STRING)")
        if hasattr(F, "try_aes_decrypt") is False
        else F.try_aes_decrypt(F.col("encrypted_pan"), F.lit(aes_key).cast("binary"), F.lit("GCM")).cast("string"),
    )


# ==============================================================================
# 4. explain("formatted") & Spark UI Diagnostics
# ==============================================================================
# - In `df.explain("formatted")`, verify `aes_encrypt`, `sha2`, and `try_aes_decrypt`
#   compile into native Catalyst `Project` expressions (no `BatchEvalPython` UDFs).
# - Ensure `Project` drops `card_number` BEFORE any `Exchange` (shuffle) or `WriteFiles`
#   node so cleartext PANs never touch shuffle spill files or Delta logs.


# ==============================================================================
# 5. Production & Interview Gotchas
# ==============================================================================
# - Gotcha 1: `aes_encrypt(..., 'GCM')` uses a random IV per row, producing distinct
#   ciphertext for identical PANs; therefore you MUST use salted `sha2` for joins/group-bys.
# - Gotcha 2: Store a `key_version` column alongside `encrypted_pan` so zero-downtime
#   key rotation can decrypt old rows with v1 while writing new rows with v2.
# - Gotcha 3: `try_aes_decrypt` returns `NULL` on wrong keys or tampered GCM tags under
#   Spark 4.0 ANSI mode, whereas `aes_decrypt` raises an exception and aborts the stage.


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("Adapter_090_PII_Encryption").getOrCreate()
    spark.conf.set("spark.sql.ansi.enabled", "true")

    key, salt_val = resolve_crypto_material()
    df_cards = spark.createDataFrame(
        [("TXN-1", "4111111111111234", 150.00), ("TXN-2", "5500000000005678", 320.50)],
        "txn_id STRING, card_number STRING, amount DOUBLE",
    )
    df_sec = encrypt_and_tokenize_pans(df_cards, key, salt_val)
    df_sec.select("txn_id", "amount", "pan_token").show(truncate=False)

    df_dec_sql = decrypt_pans_ansi_safe(df_sec, key)
    df_dec_sql.select("txn_id", "decrypted_pan", "amount", "pan_token").show(truncate=False)
