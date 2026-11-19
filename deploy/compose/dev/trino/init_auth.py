"""Prepare what Trino needs to require sign-in. Runs once at every start, as root.

1. A TLS certificate, made the first time only. Trino refuses passwords over plain HTTP.
   The certificate is self-signed; clients trust it through /tls/ca.crt.
2. The password file, rewritten every time from the passwords in the environment.

Only the three service accounts get passwords. The `role_*` users have none on
purpose: nobody can sign in as a role. The platform signs in as itself and then acts
as a role, which Trino's access rules allow for that one account only.
"""

from __future__ import annotations

import ipaddress
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import bcrypt
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

TRINO_UID = 1000
TLS = Path("/tls")
AUTH = Path("/auth")
ACCOUNTS = {
    "svc_pipeline": "TRINO_PIPELINE_PASSWORD",
    "svc_catalog": "TRINO_CATALOG_PASSWORD",
    "svc_datalake": "TRINO_APP_PASSWORD",
}


def make_certificate() -> None:
    bundle = TLS / "trino.pem"
    if bundle.exists():
        print("certificate already exists")
        return
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "trino")])
    now = datetime.now(UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=730))
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("trino"),
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                ]
            ),
            critical=False,
        )
        # Self-signed and its own trust anchor.
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .sign(key, hashes.SHA256())
    )
    cert_pem = certificate.public_bytes(serialization.Encoding.PEM)
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    # The private key is readable by Trino's user only; the certificate by anyone.
    bundle.write_bytes(key_pem + cert_pem)
    os.chown(bundle, TRINO_UID, TRINO_UID)
    bundle.chmod(0o600)
    (TLS / "ca.crt").write_bytes(cert_pem)
    (TLS / "ca.crt").chmod(0o644)
    print("made a new certificate")


def password_line(user: str, password: str) -> str:
    """One line of Trino's password file: the user and a bcrypt hash.

    Trino only recognises bcrypt hashes written with the `$2y$` prefix. The Python
    library writes `$2b$`, which is the same algorithm under a newer name.
    """
    hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt(rounds=10)).decode()
    return f"{user}:{hashed.replace('$2b$', '$2y$', 1)}"


def make_password_file() -> None:
    lines = [password_line(user, os.environ[variable]) for user, variable in ACCOUNTS.items()]
    target = AUTH / "password.db"
    target.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.chown(target, TRINO_UID, TRINO_UID)
    target.chmod(0o600)
    print(f"wrote passwords for {', '.join(ACCOUNTS)}")


if __name__ == "__main__":
    make_certificate()
    make_password_file()
