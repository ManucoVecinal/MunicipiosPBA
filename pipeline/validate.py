# -*- coding: utf-8 -*-
"""
Validaciones simples para PDFs antes de ingresar al pipeline.
"""
from __future__ import annotations


def validate_pdf_bytes(pdf_bytes: bytes, max_mb: int = 20) -> None:
    """
    Valida que el archivo sea un PDF razonable.

    Lanza ValueError con un mensaje claro si falla.
    """
    if pdf_bytes is None:
        raise ValueError("No se recibio ningun archivo.")

    if not isinstance(pdf_bytes, (bytes, bytearray)):
        raise ValueError("El archivo no esta en formato bytes.")

    size = len(pdf_bytes)
    if size == 0:
        raise ValueError("El archivo esta vacio.")

    max_bytes = max_mb * 1024 * 1024
    if size > max_bytes:
        raise ValueError(
            f"El archivo supera el tamano maximo permitido ({max_mb} MB)."
        )

    # Chequeo minimo de firma PDF
    if not pdf_bytes.startswith(b"%PDF"):
        raise ValueError("El archivo no parece ser un PDF valido (%PDF faltante).")
