# scraper-seace/scrapers/seace_logic.py

import asyncio
import re
from datetime import datetime
from urllib.parse import urljoin
from typing import List, Dict, Optional
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright


# ==============================
# CONFIGURACIÓN GLOBAL
# ==============================

SEACE_URL = "https://prod6.seace.gob.pe/buscador-publico/contrataciones"


# ==============================
# FUNCIONES DE PARSEO DE FECHA
# ==============================

def parse_fecha_seace(fecha_str: str) -> Optional[datetime]:
    """
    Parsea fecha del formato SEACE: 'dd/mm/yyyy HH:MM:SS' o 'dd/mm/yyyy'
    """
    try:
        fecha_str = fecha_str.replace("Fecha de publicación:", "").strip()
        if ' ' in fecha_str:
            return datetime.strptime(fecha_str.split()[0], "%d/%m/%Y")
        else:
            return datetime.strptime(fecha_str, "%d/%m/%Y")
    except Exception:
        return None


def fecha_en_rango(fecha_str: str, fecha_inicio: str, fecha_fin: str) -> bool:
    """
    Verifica si una fecha está en el rango especificado (INCLUSIVO)
    """
    fecha = parse_fecha_seace(fecha_str)
    if not fecha:
        return False

    inicio = parse_fecha_seace(fecha_inicio)
    fin = parse_fecha_seace(fecha_fin)

    if not inicio or not fin:
        return False

    return inicio <= fecha <= fin


# ==============================
# EXTRACCIÓN PRINCIPAL
# ==============================

async def scrape_seace_playwright(
    fecha_inicio: str,
    fecha_fin: str,
    max_paginas: int,
    page_size: int
) -> List[Dict]:
    """
    Scraper del SEACE que extrae licitaciones y filtra por fecha de publicación

    Args:
        fecha_inicio: Fecha inicial en formato dd/mm/yyyy
        fecha_fin: Fecha final en formato dd/mm/yyyy
        max_paginas: Número de páginas a scrapear
        page_size: Cantidad de resultados por página (5, 10, 25, 100)
    """
    print(f"🚀 Iniciando scraper para SEACE desde {fecha_inicio} hasta {fecha_fin}...")
    print(f"⚠️  NOTA: El SEACE no tiene filtro de fecha en el formulario")
    print(f"   → Estrategia: Extraer licitaciones recientes y filtrar localmente")
    print(f"⚙️  Configuración: {page_size} resultados/página, revisando {max_paginas} páginas.\n")

    todas_licitaciones = []
    licitaciones_en_rango = []
    page_count = 1
    current_page_size_text = "5"

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox", "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage", "--disable-gpu",
                    "--disable-software-rasterizer"
                ]
            )
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            page = await context.new_page()

            # 1. Cargar página
            print("   ⏳ Cargando SEACE...")
            await page.goto(SEACE_URL, wait_until="networkidle", timeout=60000)
            print("   ✅ Página cargada")

            # 2. Esperar que Angular cargue
            print("   ⏳ Esperando aplicación Angular...")
            await page.wait_for_timeout(5000)

            # Configurar tamaño de página
            try:
                await page.wait_for_selector("span.mat-mdc-select-min-line", timeout=10000)
                current_size_elem = page.locator("span.mat-mdc-select-min-line").first
                current_page_size_text = await current_size_elem.inner_text()

                if int(current_page_size_text) == page_size:
                    print(f"   ℹ️  El tamaño de página ya está configurado en {page_size}. Omitiendo clic.")
                else:
                    print(f"   ⚙️  Cambiando tamaño de página de {current_page_size_text} a {page_size}...")
                    await page.locator("mat-select[aria-labelledby*='mat-paginator-page-size-label']").click()
                    await page.wait_for_timeout(500)
                    await page.locator(f"mat-option[role='option']:has-text('{page_size}')").click()
                    print("   ⏳ Esperando recarga de resultados...")
                    await page.wait_for_load_state("networkidle", timeout=10000)
                    await page.wait_for_timeout(2000)
                    print("   ✅ Tamaño de página configurado")

            except Exception as e:
                print(f"   ⚠️ No se pudo configurar el tamaño de página. Continuando con {current_page_size_text} resultados/pág.")
                print(f"      Error: {str(e).splitlines()[0]}")

            await page.wait_for_timeout(3000)

            # Verificar resultados iniciales
            print("   ⏳ Esperando resultados iniciales...")
            resultados_cargados = False
            for intento in range(5):
                try:
                    await page.wait_for_selector("div[class*='rounded']", timeout=10000)
                    resultados_cargados = True
                    print("   ✅ Resultados detectados")
                    break
                except:
                    print(f"   ⏳ Intento {intento + 1}/5...")
                    await page.wait_for_timeout(3000)

            if not resultados_cargados:
                print("   ⚠️ No se detectaron resultados. Guardando HTML...")
                content = await page.content()
                with open('debug_initial_load.html', 'w', encoding='utf-8') as f:
                    f.write(content)
                print("   🐛 Revise 'debug_initial_load.html'")
                try:
                    print("   🔄 Intentando buscar con filtros vacíos...")
                    buttons = await page.query_selector_all("button")
                    for btn in buttons:
                        text = await btn.inner_text()
                        if 'Buscar' in text or 'buscar' in text.lower():
                            await btn.click()
                            await page.wait_for_timeout(5000)
                            break
                except:
                    pass

            # Extracción con paginación
            while page_count <= max_paginas:
                print(f"\n   📄 === PÁGINA {page_count} / {max_paginas} ===")

                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(1500)
                await page.evaluate("window.scrollTo(0, 0)")
                await page.wait_for_timeout(1000)

                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')

                cards = []
                selectores_posibles = [
                    "div.bg-fondo-section", "div[class*='bg-fondo']",
                    "div.rounded-md", "app-card-contratacion", "div[class*='card']",
                ]
                for selector in selectores_posibles:
                    cards = soup.select(selector)
                    if len(cards) > 0:
                        print(f"      ✅ Selector '{selector}' → {len(cards)} tarjetas")
                        break

                if not cards:
                    print(f"      ⚠️ No se encontraron tarjetas")
                    with open(f'debug_page_{page_count}.html', 'w', encoding='utf-8') as f:
                        f.write(content)
                    print(f"      🐛 HTML guardado: debug_page_{page_count}.html")
                    if page_count == 1:
                        print("\n   ❌ No se detectó estructura de licitaciones en la primera página")
                        break
                    else:
                        print("   📋 Fin de resultados")
                        break

                for idx, card in enumerate(cards, 1):
                    try:
                        card_text = card.get_text(separator="\n", strip=True)
                        codigo_proceso = "No disponible"
                        codigo_elem = card.select_one("p.font-semibold")
                        if codigo_elem:
                            codigo_proceso = codigo_elem.get_text(strip=True)

                        estado = "No disponible"
                        estado_elem = card.select_one("span[class*='bg-']")
                        if estado_elem:
                            estado = estado_elem.get_text(strip=True)

                        entidad = "No disponible"
                        font_semibolds = card.select("p.font-semibold")
                        if len(font_semibolds) > 1:
                            entidad = font_semibolds[1].get_text(strip=True)
                        elif len(font_semibolds) == 1:
                            all_ps = card.select("p")
                            for p in all_ps:
                                txt = p.get_text(strip=True)
                                if len(txt) > 20 and txt != codigo_proceso and not any(
                                    x in txt.lower() for x in ['servicio:', 'bien:', 'obra:', 'fecha']
                                ):
                                    entidad = txt
                                    break

                        descripcion = "No disponible"
                        for p in card.select("p"):
                            txt = p.get_text(strip=True)
                            if any(prefix in txt for prefix in ["Servicio:", "Bien:", "Obra:", "Consultoría:", "Consultoria:"]):
                                descripcion = txt
                                for prefix in ["Servicio:", "Bien:", "Obra:", "Consultoría:", "Consultoria:"]:
                                    descripcion = descripcion.replace(prefix, "").strip()
                                break

                        fecha_pub = "No disponible"
                        for p in card.select("p"):
                            txt = p.get_text(strip=True)
                            if "Fecha de publicación:" in txt:
                                fecha_pub = txt.replace("Fecha de publicación:", "").strip()
                                break

                        fechas_cot = "No disponible"
                        for p in card.select("p"):
                            txt = p.get_text(strip=True)
                            if "Cotizaciones:" in txt or "Cotización:" in txt:
                                fechas_cot = txt.replace("Cotizaciones:", "").replace("Cotización:", "").strip()
                                break

                        enlace = ""
                        enlace_elem = card.select_one("a[href*='/buscador-publico/contrataciones/']")
                        if not enlace_elem:
                            enlace_elem = card.select_one("a[href*='/contrataciones/']")
                        if enlace_elem and enlace_elem.get('href'):
                            enlace = urljoin(SEACE_URL, enlace_elem['href'])

                        licitacion = {
                            "Codigo Proceso": codigo_proceso,
                            "Entidad": entidad,
                            "Descripcion": descripcion,
                            "Estado": estado,
                            "Fecha Publicacion": fecha_pub,
                            "Fechas Cotizacion": fechas_cot,
                            "Enlace Detalle": enlace,
                            "CUBSO": "",
                            "Segmento": ""
                        }

                        todas_licitaciones.append(licitacion)

                        if fecha_pub != "No disponible":
                            if fecha_en_rango(fecha_pub, fecha_inicio, fecha_fin):
                                licitaciones_en_rango.append(licitacion)

                    except Exception as e:
                        print(f"      ❌ Error en tarjeta {idx}: {e}")
                        continue

                print(f"      ✅ Extraídas: {len(cards)} | Total: {len(todas_licitaciones)} | En rango: {len(licitaciones_en_rango)}")

                # Paginación
                if page_count >= max_paginas:
                    print(f"   📋 Límite de páginas ({max_paginas}) alcanzado")
                    break

                try:
                    next_btn = None
                    next_selectors = [
                        "button[aria-label='Siguiente página']",
                        "button[aria-label*='Next']",
                        "button:has-text('›')",
                        "button:has-text('Siguiente')"
                    ]
                    for selector in next_selectors:
                        try:
                            btn = page.locator(selector).first
                            if await btn.is_visible(timeout=2000):
                                next_btn = btn
                                break
                        except:
                            continue

                    if not next_btn:
                        print("   📋 No hay botón de siguiente página")
                        break

                    if await next_btn.is_disabled():
                        print("   📋 Última página alcanzada")
                        break

                    print("   ➡️  Siguiente página...")
                    await next_btn.click()
                    await page.wait_for_load_state("networkidle", timeout=10000)
                    await page.wait_for_timeout(2000)
                    page_count += 1

                except Exception as e:
                    print(f"   ℹ️  Fin de paginación: {str(e)[:80]}")
                    break

            await browser.close()

            print(f"\n{'='*70}")
            print(f"✅ EXTRACCIÓN COMPLETADA")
            print(f"{'='*70}")
            print(f"📊 Total extraídas: {len(todas_licitaciones)}")
            print(f"✅ En rango de fechas ({fecha_inicio} → {fecha_fin}): {len(licitaciones_en_rango)}")
            print(f"📄 Páginas procesadas: {page_count-1} / {max_paginas}")
            print(f"{'='*70}\n")

            return licitaciones_en_rango

    except Exception as e:
        print(f"\n❌ Error fatal: {e}")
        import traceback
        traceback.print_exc()
        return []


# ==============================
# EXTRACCIÓN DE CUBSO
# ==============================

async def bloquear_recursos_innecesarios(route):
    """Bloquea la carga de imágenes, css y fuentes"""
    if route.request.resource_type in {"image", "stylesheet", "font"}:
        await route.abort()
    else:
        await route.continue_()


async def extraer_cubso_individual(context, url: str) -> str:
    page = None
    try:
        page = await context.new_page()
        await page.route("**/*", bloquear_recursos_innecesarios)
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)

        cubso = None
        try:
            cubso_elem = page.locator("td.mat-column-codCubso, td[class*='codCubso']").first
            cubso_text = await cubso_elem.inner_text(timeout=7000)
            if cubso_text:
                cubso = cubso_text.strip()
        except:
            pass

        if not cubso:
            content = await page.content()
            soup = BeautifulSoup(content, 'html.parser')
            for td in soup.find_all('td'):
                txt = td.get_text(strip=True)
                if txt and txt.isdigit() and len(txt) >= 10:
                    parent_row = td.find_parent('tr')
                    if parent_row:
                        table = td.find_parent('table')
                        if table:
                            headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
                            if any('cubso' in h for h in headers):
                                cubso = txt
                                break

        if not cubso:
            content = await page.content()
            match = re.search(r'CUBSO[:\s]*(\d{10,})', content, re.IGNORECASE)
            if match:
                cubso = match.group(1)

        await page.close()
        return cubso.strip() if cubso else "No disponible"

    except Exception:
        if page:
            await page.close()
        return "Error"


async def extraer_cubso_batch(enlaces: List[str], max_concurrent: int = 5) -> Dict[str, str]:
    print(f"\n🔍 Extrayendo códigos CUBSO de {len(enlaces)} licitaciones...")
    resultados = {}

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
            )
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )

            for i in range(0, len(enlaces), max_concurrent):
                lote = enlaces[i:i + max_concurrent]
                print(f"   📦 Procesando lote {i//max_concurrent + 1}/{(len(enlaces)//max_concurrent) + 1} ({len(lote)} licitaciones)...")

                tasks = []
                for url in lote:
                    if url:
                        tasks.append(extraer_cubso_individual(context, url))

                resultados_lote = await asyncio.gather(*tasks, return_exceptions=True)

                for url, resultado in zip(lote, resultados_lote):
                    if isinstance(resultado, Exception):
                        resultados[url] = "Error"
                    else:
                        resultados[url] = resultado

                cubsos_ok_lote = sum(1 for v in resultados_lote if v not in [None, "Error", "No disponible", ""])
                print(f"      ✅ CUBSO extraídos (en este lote): {cubsos_ok_lote}/{len(lote)}")
                await asyncio.sleep(0.5)

            await browser.close()
            print("   ✅ Extracción de CUBSO completada\n")
            return resultados

    except Exception as e:
        print(f"   ❌ Error en extracción de CUBSO: {e}")
        return resultados


# ==============================
# FILTROS Y UTILIDADES
# ==============================

def aplicar_filtros_licitaciones_seace(df: pd.DataFrame, keyword: str, objeto_tipo: List[str]) -> pd.DataFrame:
    df_f = df.copy()
    if keyword.strip():
        k = keyword.strip().lower()
        df_f = df_f[
            df_f['Codigo Proceso'].str.lower().str.contains(k, na=False, regex=False) |
            df_f['Entidad'].str.lower().str.contains(k, na=False, regex=False) |
            df_f['Descripcion'].str.lower().str.contains(k, na=False, regex=False)
        ]
    if objeto_tipo:
        patron = '|'.join([t.lower() for t in objeto_tipo])
        df_f = df_f[df_f['Descripcion'].str.lower().str.contains(patron, na=False, regex=True)]
    return df_f


def determinar_sector(desc: str) -> str:
    d = desc.lower()
    if any(w in d for w in ['obra', 'construcc', 'vial', 'edificación', 'infraestructura']):
        return 'INFRAESTRUCTURA/OBRA'
    if any(w in d for w in ['servicio', 'consultor', 'asesor', 'mantenimiento']):
        return 'SERVICIOS/CONSULTORÍA'
    if any(w in d for w in ['bien', 'adquisic', 'equipo', 'material', 'suministro']):
        return 'BIENES'
    if any(w in d for w in ['software', 'sistema', 'ti', 'tecnología', 'informática']):
        return 'TECNOLOGÍA'
    return "OTROS"