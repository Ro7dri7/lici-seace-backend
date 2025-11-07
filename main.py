# scraper-seace/main.py

import asyncio
import pandas as pd
import sys
from datetime import datetime
from scrapers.seace_logic import (
    scrape_seace_playwright,
    extraer_cubso_batch,
    determinar_sector
)


def validar_fecha(fecha_str: str) -> bool:
    """Valida si una cadena tiene formato dd/mm/yyyy"""
    try:
        datetime.strptime(fecha_str, "%d/%m/%Y")
        return True
    except ValueError:
        return False


async def main():
    # 🔧 Configuración por defecto
    FECHA_INICIO_DEFECTO = "27/10/2025"
    FECHA_FIN_DEFECTO = "02/11/2025"
    MAX_PAGINAS = 80               # Aumentado para obtener +600 resultados
    PAGE_SIZE = 100                # Máximo por página

    # 📥 Leer fechas desde argumentos de línea de comandos
    if len(sys.argv) == 3:
        FECHA_INICIO = sys.argv[1]
        FECHA_FIN = sys.argv[2]
        if not (validar_fecha(FECHA_INICIO) and validar_fecha(FECHA_FIN)):
            print("❌ Error: Las fechas deben tener el formato dd/mm/yyyy")
            print("Ejemplo: python3 main.py 27/10/2025 02/11/2025")
            return
        print(f"📅 Usando rango personalizado: {FECHA_INICIO} → {FECHA_FIN}")
    else:
        FECHA_INICIO = FECHA_INICIO_DEFECTO
        FECHA_FIN = FECHA_FIN_DEFECTO
        print(f"📅 Usando rango por defecto: {FECHA_INICIO} → {FECHA_FIN}")

    # 🚀 Ejecutar scraper principal
    print("🔍 Iniciando scraping de licitaciones...")
    print(f"⚙️  Configuración: {PAGE_SIZE} resultados/página, {MAX_PAGINAS} páginas máx.")
    licitaciones = await scrape_seace_playwright(
        fecha_inicio=FECHA_INICIO,
        fecha_fin=FECHA_FIN,
        max_paginas=MAX_PAGINAS,
        page_size=PAGE_SIZE
    )

    if not licitaciones:
        print("⚠️ No se encontraron licitaciones en el rango especificado.")
        return

    # 🔄 Extraer CUBSO de los enlaces
    enlaces = [lic["Enlace Detalle"] for lic in licitaciones if lic["Enlace Detalle"]]
    print(f"\n🔗 Extrayendo CUBSO de {len(enlaces)} enlaces...")
    cubso_dict = await extraer_cubso_batch(enlaces, max_concurrent=10)  # Aumentado de 5 a 10

    # 🧩 Combinar CUBSO y determinar sector
    for lic in licitaciones:
        url = lic["Enlace Detalle"]
        lic["CUBSO"] = cubso_dict.get(url, "No disponible")
        lic["Segmento"] = determinar_sector(lic["Descripcion"])

    # 📊 Convertir a DataFrame y guardar
    df = pd.DataFrame(licitaciones)
    output_file = f"licitaciones_seace_{FECHA_INICIO.replace('/', '')}-{FECHA_FIN.replace('/', '')}.csv"
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"\n✅ Resultados guardados en: {output_file}")
    print(f"📊 Total de licitaciones procesadas: {len(df)}")


if __name__ == "__main__":
    asyncio.run(main())