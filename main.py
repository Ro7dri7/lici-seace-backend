# scraper-seace/main.py

import asyncio
import pandas as pd
from scrapers.seace_logic import (
    scrape_seace_playwright,
    extraer_cubso_batch,
    determinar_sector
)


async def main():
    # 🔧 Configuración
    FECHA_INICIO = "01/01/2025"   # Formato: dd/mm/yyyy
    FECHA_FIN = "31/12/2025"
    MAX_PAGINAS = 10               # Ajusta según necesidad
    PAGE_SIZE = 100                # Opciones: 5, 10, 25, 100

    # 🚀 Ejecutar scraper principal
    print("🔍 Iniciando scraping de licitaciones...")
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
    cubso_dict = await extraer_cubso_batch(enlaces, max_concurrent=5)

    # 🧩 Combinar CUBSO y determinar sector
    for lic in licitaciones:
        url = lic["Enlace Detalle"]
        lic["CUBSO"] = cubso_dict.get(url, "No disponible")
        lic["Segmento"] = determinar_sector(lic["Descripcion"])

    # 📊 Convertir a DataFrame y guardar
    df = pd.DataFrame(licitaciones)
    output_file = "licitaciones_seace.csv"
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"\n✅ Resultados guardados en: {output_file}")
    print(f"📊 Total de licitaciones procesadas: {len(df)}")


if __name__ == "__main__":
    asyncio.run(main())