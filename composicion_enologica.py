# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from datetime import datetime
import sqlalchemy
from sqlalchemy.types import Integer, String, Float, DateTime, BigInteger, Numeric
from sqlalchemy import text, create_engine
from sqlalchemy.engine import Connection
import os
import getpass
from pathlib import Path
import math
import traceback

# --- Constantes ---
MAX_LEN_D_DEPOSITO = 20
COLUMNAS_OT = ['C_TAREA', 'D_TAREA', 'OBS_DESTINO', 'OBS_GENERALES', 'OBS_ORIGEN', 'CANT_ART_DESTINO', 'CANT_ART_ORIGEN']

# --- Helper Function para Chunking ---
def ejecutar_consulta_con_chunks(
    sql_select_part: str, id_column_name_in_sql: str, id_list: list,
    chunk_size: int, connection: Connection, params: dict = None,
    where_clause_base: str = ""
) -> pd.DataFrame:
    unique_ids = [str(item) for item in pd.Series(id_list).dropna().unique().tolist()]
    if not unique_ids:
        try:
            limit_sql = sql_select_part.replace("SELECT", "SELECT /*+ FIRST_ROWS(1) */", 1)
            test_sql = f"{limit_sql} WHERE 1=0 {where_clause_base}"
            empty_df = pd.read_sql(text(test_sql), connection, params=params if params else {})
            empty_df.columns = empty_df.columns.str.lower()
            return empty_df
        except Exception:
             return pd.DataFrame()
    all_results = []
    num_chunks = math.ceil(len(unique_ids) / chunk_size)
    base_params = params.copy() if params else {}
    for i in range(num_chunks):
        start_index = i * chunk_size
        end_index = start_index + chunk_size
        chunk_ids_current = unique_ids[start_index:end_index]
        if not chunk_ids_current: continue
        placeholders = [f":chunk_id_{j}" for j in range(len(chunk_ids_current))]
        chunk_params = {}
        for j, chunk_id_str in enumerate(chunk_ids_current):
            try: chunk_params[f"chunk_id_{j}"] = int(chunk_id_str)
            except ValueError: chunk_params[f"chunk_id_{j}"] = chunk_id_str
        current_params = base_params.copy()
        current_params.update(chunk_params)
        
        sql_base_upper = sql_select_part.upper()
        if "WHERE" in sql_base_upper:
             sql_chunk = f"{sql_select_part} {where_clause_base} AND {id_column_name_in_sql} IN ({', '.join(placeholders)})"
        else:
             sql_chunk = f"{sql_select_part} WHERE {id_column_name_in_sql} IN ({', '.join(placeholders)}) {where_clause_base}"

        try:
            df_chunk = pd.read_sql(text(sql_chunk), connection, params=current_params)
            all_results.append(df_chunk)
        except Exception as e:
            print(f"Error ejecutando chunk {i+1}/{num_chunks} para '{id_column_name_in_sql}': {e}")
            continue
    if not all_results:
        try:
            limit_sql = sql_select_part.replace("SELECT", "SELECT /*+ FIRST_ROWS(1) */", 1)
            test_sql = f"{limit_sql} WHERE 1=0 {where_clause_base}"
            empty_df = pd.read_sql(text(test_sql), connection, params=base_params)
            empty_df.columns = empty_df.columns.str.lower()
            return empty_df
        except Exception: return pd.DataFrame()
    concatenated_df = pd.concat(all_results, ignore_index=True)
    concatenated_df.columns = concatenated_df.columns.str.lower()
    return concatenated_df

# --- Función de ayuda para enriquecer con datos de OT ---
def _enriquecer_con_ordenes_trabajo(df_principal: pd.DataFrame, engine: sqlalchemy.engine.Engine) -> pd.DataFrame:
    if df_principal.empty or 'MOS_ID' not in df_principal.columns:
        for col in COLUMNAS_OT:
            df_principal[col] = None
        return df_principal

    lista_mos_ids = df_principal['MOS_ID'].dropna().unique().tolist()
    if not lista_mos_ids:
        for col in COLUMNAS_OT:
            df_principal[col] = None
        return df_principal

    vista_ordenes = "VZ_APX_ORDENES_TRABAJO"
    sql_ordenes_part = f"SELECT ID_CIERRE, {', '.join(COLUMNAS_OT)} FROM {vista_ordenes}"
    
    with engine.connect() as connection:
        df_ordenes = ejecutar_consulta_con_chunks(sql_ordenes_part, "ID_CIERRE", lista_mos_ids, 999, connection)

    if df_ordenes.empty:
        for col in COLUMNAS_OT:
            df_principal[col] = None
        return df_principal

    df_ordenes = df_ordenes.rename(columns={'id_cierre': 'MOS_ID'})
    df_ordenes['MOS_ID'] = pd.to_numeric(df_ordenes['MOS_ID'], errors='coerce')
    df_principal['MOS_ID'] = pd.to_numeric(df_principal['MOS_ID'], errors='coerce')

    df_enriquecido = pd.merge(df_principal, df_ordenes, on='MOS_ID', how='left')
    
    return df_enriquecido

# --- Lógica de Procesamiento ---
def procesar_descubes(
    datos_movim: dict,
    df_lotes: pd.DataFrame,
    df_cl: pd.DataFrame,
    df_depositos: pd.DataFrame
) -> pd.DataFrame:
    """
    Descubes (C_TIPO_COMPRO = 28) SIN cruce con COSECHA_*, COSECHA_DEPOSITO,
    COSECHA_CUARTEL ni depósitos. Solo LOTES origen/destino y cantidades.

    Regla:
      - Para cada MOS_ID (28), sumar Q_ARTICULO por C_LOTE en DET_MOV_STOCK.
      - El C_LOTE con mayor suma es el DESTINO.
      - Los demás C_LOTE del mismo MOS_ID son ORÍGENES.
      - Se genera ORIGEN -> DESTINO con CANTIDAD = suma(Q_ARTICULO) del ORIGEN.
    """
    import pandas as pd
    if not isinstance(datos_movim, dict) or "movim_stock" not in datos_movim or "det_mov_stock" not in datos_movim:
        return pd.DataFrame()

    df_ms  = datos_movim["movim_stock"].copy()
    df_dms = datos_movim["det_mov_stock"].copy()

    # Normaliza nombres
    df_ms.columns  = df_ms.columns.str.lower()
    df_dms.columns = df_dms.columns.str.lower()
    if "id" in df_ms.columns:  df_ms  = df_ms.rename(columns={"id": "mos_id"})
    if "id" in df_dms.columns: df_dms = df_dms.rename(columns={"id": "dms_id"})

    # Filtra por tipo 28 por seguridad
    df_ms = df_ms[df_ms.get("c_tipo_compro").astype("Int64") == 28]

    # Tipos
    for c in ("mos_id","c_lote","q_articulo"):
        if c in df_dms.columns: df_dms[c] = pd.to_numeric(df_dms[c], errors="coerce")
    if df_dms.empty or df_ms.empty:
        return pd.DataFrame()

    # Sumas por MOS_ID + C_LOTE
    g = (df_dms.groupby(["mos_id","c_lote"], dropna=False)["q_articulo"]
               .sum().reset_index().rename(columns={"q_articulo":"sum_q"}))

    # Destino = mayor sum_q
    g["rk"] = g.groupby("mos_id")["sum_q"].rank(method="first", ascending=False)
    destinos = g[g["rk"] == 1.0][["mos_id","c_lote"]].rename(columns={"c_lote":"c_lote_destino"})

    # Orígenes = resto
    pares = g.merge(destinos, on="mos_id", how="left")
    pares = pares[pares["c_lote"] != pares["c_lote_destino"]].copy()

    # Añade fecha/tipo
    fechas = df_ms[["mos_id","f_movimiento","c_tipo_compro"]].drop_duplicates()
    pares  = pares.merge(fechas, on="mos_id", how="left")

    # Construye salida SOLO con lo permitido y el resto NULL
    out = pd.DataFrame({
        "C_LOTE"       : pd.to_numeric(pares["c_lote_destino"], errors="coerce").astype("Int64"),
        "C_VARIEDAD_INV": None,
        "C_PERIODO"    : None,
        "ID_SUBVALLE"  : None,
        "CANTIDAD"     : pares["sum_q"].astype(float),
        "CLAVE_EXT_LOTE": None,
        "MOS_ID"       : pd.to_numeric(pares["mos_id"], errors="coerce").astype("Int64"),
        "ID"           : None,
        "C_TIPO_COMPRO": pd.to_numeric(pares["c_tipo_compro"], errors="coerce").astype("Int64"),
        "F_MOVIMIENTO" : pd.to_datetime(pares["f_movimiento"], errors="coerce"),
        "C_LOTE_ORIGEN": pd.to_numeric(pares["c_lote"], errors="coerce").astype("Int64"),
        "PORCENTAJE_SI": None,
        "CIU_NUMERO"   : None,
        "NRO_INSCRIPCION": None,
        "COD_CUARTEL"  : None,
        "CUARTEL_LOG"  : None,
        "D_LOTE"       : None,
        "C_DORIGEN"    : None,
        "D_DORIGEN"    : None,
        "C_DDESTINO"   : None,
        "D_DDESTINO"   : None,
        "ORIGEN"       : "Descube"
    })

    # Orden estándar esperado por APX_TRAZA_DETALLE
    cols_finales = [
        "C_LOTE","C_VARIEDAD_INV","C_PERIODO","ID_SUBVALLE","CANTIDAD","CLAVE_EXT_LOTE",
        "MOS_ID","ID","C_TIPO_COMPRO","F_MOVIMIENTO","C_LOTE_ORIGEN","PORCENTAJE_SI",
        "CIU_NUMERO","NRO_INSCRIPCION","COD_CUARTEL","CUARTEL_LOG","D_LOTE",
        "C_DORIGEN","D_DORIGEN","C_DDESTINO","D_DDESTINO","ORIGEN"
    ]
    out = out.reindex(columns=cols_finales)
    return out

def procesar_compras(engine: sqlalchemy.engine.Engine, fecha_desde_str: str, fecha_fin_str: str, db_user: str, df_lotes: pd.DataFrame, df_depositos: pd.DataFrame):
    yield "\n--- Iniciando Procesamiento de Compras (Tipo 13) ---"
    with engine.connect() as connection:
        sql_factura_compra = f"""SELECT ID, F_FACTURA, C_TIPO_COMPRO FROM FACTURA_COMPRAS WHERE C_TIPO_COMPRO = 13 AND F_FACTURA >= TO_DATE(:f_ini, 'YYYY-MM-DD') AND F_FACTURA < TO_DATE(:f_fin, 'YYYY-MM-DD') + 1"""
        df_fc = pd.read_sql(text(sql_factura_compra), connection, params={'f_ini': fecha_desde_str, 'f_fin': fecha_fin_str})
        if df_fc.empty:
            yield "No se encontraron compras en el período."
            return pd.DataFrame()
        original_fc_cols = df_fc.columns.tolist()
        rename_map_fc = {}
        id_col_original = next((col for col in original_fc_cols if col.lower() == 'id'), None)
        if id_col_original: rename_map_fc[id_col_original] = 'fac_id_header'
        else:
            yield f"Error Crítico Compras: No se encontró la columna 'ID'. Columnas: {original_fc_cols}"
            return pd.DataFrame()
        for col in original_fc_cols:
            if col != id_col_original: rename_map_fc[col] = col.lower()
        df_fc = df_fc.rename(columns=rename_map_fc)
        if 'fac_id_header' not in df_fc.columns:
            yield f"Error Inesperado Compras: Falló renombrado. Columnas: {df_fc.columns.tolist()}"
            return pd.DataFrame()
        lista_fac_id = df_fc['fac_id_header'].dropna().unique().tolist()
        sql_det_fc_base = "SELECT FAC_ID, ID, C_LOTE_STOCK, Q_ARTICULO, C_ARTICULO, COSECHA, C_DEPOSITO FROM DET_FAC_COM"
        df_det_fc = ejecutar_consulta_con_chunks(sql_det_fc_base, "FAC_ID", lista_fac_id, 999, connection)
        if df_det_fc.empty: return pd.DataFrame()
        df_det_fc = df_det_fc.rename(columns={'id': 'det_fac_id'}, errors='ignore')
        if 'c_deposito' not in df_det_fc.columns:
            yield "Advertencia Compras: No se encontró 'c_deposito' en DET_FAC_COM."
            df_det_fc['c_deposito'] = None
        lista_articulos = df_det_fc['c_articulo'].dropna().unique().tolist()
        sql_items_base = "SELECT C_ARTICULO, C_TEMPORADA, TIPO_CLASIF FROM ITEMS"
        df_items = ejecutar_consulta_con_chunks(sql_items_base, "C_ARTICULO", lista_articulos, 999, connection, where_clause_base="AND TIPO_CLASIF IN (4, 14)")
        if df_items.empty: return pd.DataFrame()
        df_det_fc['fac_id'] = pd.to_numeric(df_det_fc['fac_id'], errors='coerce')
        df_fc['fac_id_header'] = pd.to_numeric(df_fc['fac_id_header'], errors='coerce')
        df_merged = pd.merge(df_det_fc, df_fc, left_on='fac_id', right_on='fac_id_header', suffixes=('_det', '_fac'))
        df_merged = pd.merge(df_merged, df_items[['c_articulo', 'c_temporada']], on='c_articulo', how='inner')
        df_merged['c_lote_stock'] = pd.to_numeric(df_merged['c_lote_stock'], errors='coerce')
        df_lotes['c_lote'] = pd.to_numeric(df_lotes['c_lote'], errors='coerce')
        lotes_cols_sel = ['c_lote', 'clave_externa', 'id_subvalle']
        if 'd_lote' in df_lotes.columns: lotes_cols_sel.append('d_lote')
        df_merged = pd.merge(df_merged, df_lotes[lotes_cols_sel], left_on='c_lote_stock', right_on='c_lote', how='left', suffixes=('', '_lote'))
        if 'c_deposito' in df_merged.columns:
            df_depositos_sel = df_depositos[['c_deposito', 'd_deposito']].rename(columns={'d_deposito': 'd_dorigen'})
            df_merged['c_deposito'] = pd.to_numeric(df_merged['c_deposito'], errors='coerce')
            df_depositos_sel['c_deposito'] = pd.to_numeric(df_depositos_sel['c_deposito'], errors='coerce')
            df_merged = pd.merge(df_merged, df_depositos_sel, on='c_deposito', how='left')
        else: df_merged['d_dorigen'] = None
        df_composicion = pd.DataFrame()
        df_composicion['C_LOTE'] = df_merged['c_lote_stock']
        df_composicion['C_VARIEDAD_INV'] = df_merged['c_temporada']
        df_composicion['C_PERIODO'] = df_merged['cosecha']
        df_composicion['ID_SUBVALLE'] = df_merged['id_subvalle']
        df_composicion['CANTIDAD'] = df_merged['q_articulo']
        df_composicion['CLAVE_EXT_LOTE'] = df_merged['clave_externa']
        df_composicion['MOS_ID'] = df_merged['fac_id']
        df_composicion['ID'] = df_merged['det_fac_id']
        df_composicion['C_TIPO_COMPRO'] = df_merged['c_tipo_compro']
        df_composicion['F_MOVIMIENTO'] = df_merged['f_factura']
        df_composicion['C_LOTE_ORIGEN'] = None
        df_composicion['PORCENTAJE_SI'] = None
        df_composicion['CIU_NUMERO'] = None
        df_composicion['NRO_INSCRIPCION'] = None
        df_composicion['COD_CUARTEL'] = None
        df_composicion['CUARTEL_LOG'] = None
        df_composicion['D_LOTE'] = df_merged.get('d_lote')
        df_composicion['C_DORIGEN'] = df_merged.get('c_deposito')
        df_composicion['D_DORIGEN'] = df_merged.get('d_dorigen')
        df_composicion['C_DDESTINO'] = None
        df_composicion['D_DDESTINO'] = None
        df_composicion['ORIGEN'] = 'Compra'
    
        yield "Enriqueciendo compras con datos de órdenes de trabajo..."
        df_composicion = _enriquecer_con_ordenes_trabajo(df_composicion, engine)
        
        for col_lower in ['c_lote', 'mos_id', 'id', 'c_periodo', 'c_tipo_compro', 'ciu_numero', 'cod_cuartel', 'c_lote_origen', 'porcentaje_si', 'c_dorigen', 'c_ddestino']:
            col_upper = col_lower.upper()
            if col_upper in df_composicion.columns:
                try: df_composicion[col_upper] = pd.to_numeric(df_composicion[col_upper], errors='coerce').astype('Int64')
                except Exception:
                    try: df_composicion[col_upper] = pd.to_numeric(df_composicion[col_upper], errors='coerce').astype('Float64')
                    except Exception as e: yield f"Advertencia Compra: No se pudo convertir {col_lower}: {e}"
        
        if 'F_MOVIMIENTO' in df_composicion.columns: df_composicion['F_MOVIMIENTO'] = pd.to_datetime(df_composicion['F_MOVIMIENTO'], errors='coerce')
        
        string_cols_lower = ['c_variedad_inv', 'id_subvalle', 'clave_ext_lote', 'nro_inscripcion', 'cuartel_log', 'd_lote', 'origen', 'd_dorigen', 'd_ddestino'] + [c.lower() for c in COLUMNAS_OT]
        for col_lower in string_cols_lower:
            col_upper = col_lower.upper()
            if col_upper in df_composicion.columns: df_composicion[col_upper] = df_composicion[col_upper].astype(pd.StringDtype())
        
        if 'D_DORIGEN' in df_composicion.columns: df_composicion['D_DORIGEN'] = df_composicion['D_DORIGEN'].fillna('').str.slice(0, MAX_LEN_D_DEPOSITO)
        if 'D_DDESTINO' in df_composicion.columns: df_composicion['D_DDESTINO'] = df_composicion['D_DDESTINO'].fillna('').str.slice(0, MAX_LEN_D_DEPOSITO)

        if 'CANTIDAD' in df_composicion.columns: df_composicion['CANTIDAD'] = df_composicion['CANTIDAD'].astype(float)
        
        final_order = ['C_LOTE', 'C_VARIEDAD_INV', 'C_PERIODO', 'ID_SUBVALLE', 'CANTIDAD', 'CLAVE_EXT_LOTE', 'MOS_ID', 'ID', 'C_TIPO_COMPRO', 'F_MOVIMIENTO', 'C_LOTE_ORIGEN', 'PORCENTAJE_SI', 'CIU_NUMERO', 'NRO_INSCRIPCION', 'COD_CUARTEL', 'CUARTEL_LOG', 'D_LOTE', 'C_DORIGEN', 'D_DORIGEN', 'C_DDESTINO', 'D_DDESTINO', 'ORIGEN'] + COLUMNAS_OT
        for col in final_order:
            if col not in df_composicion.columns: df_composicion[col] = None
        df_composicion = df_composicion.reindex(columns=final_order)
        return df_composicion

def procesar_ajustes_inventario(engine: sqlalchemy.engine.Engine, fecha_desde_str: str, fecha_fin_str: str, db_user: str, df_lotes: pd.DataFrame, df_depositos: pd.DataFrame):
    yield "\n--- Iniciando Procesamiento de Ajustes de Inventario (Tipos 31, 95) ---"
    tipos_ajuste = [31, 95]
    
    with engine.connect() as connection:
        sql_movim_ajuste = f"SELECT ID, F_MOVIMIENTO, C_TIPO_COMPRO FROM MOVIM_STOCK WHERE C_TIPO_COMPRO IN ({','.join(map(str, tipos_ajuste))}) AND F_MOVIMIENTO >= TO_DATE(:f_ini, 'YYYY-MM-DD') AND F_MOVIMIENTO < TO_DATE(:f_fin, 'YYYY-MM-DD') + 1"
        df_ms = pd.read_sql(text(sql_movim_ajuste), connection, params={'f_ini': fecha_desde_str, 'f_fin': fecha_fin_str})
        if df_ms.empty:
            yield "No se encontraron ajustes de inventario en el período."
            return pd.DataFrame()
        
        df_ms.columns = df_ms.columns.str.lower()
        df_ms = df_ms.rename(columns={'id': 'mos_id'}, errors='ignore')

        lista_mos_id = df_ms['mos_id'].dropna().unique().tolist()
        sql_dms_base = "SELECT ID, MOS_ID, C_LOTE, C_ARTICULO, Q_ARTICULO, COSECHA, C_DEPOSITO FROM DET_MOV_STOCK"
        df_dms = ejecutar_consulta_con_chunks(sql_dms_base, "MOS_ID", lista_mos_id, 999, connection)
        if df_dms.empty: return pd.DataFrame()
        df_dms = df_dms.rename(columns={'id': 'dms_id'}, errors='ignore')

        lista_articulos = df_dms['c_articulo'].dropna().unique().tolist()
        sql_items_base = "SELECT C_ARTICULO, C_TEMPORADA, TIPO_CLASIF FROM ITEMS"
        df_items = ejecutar_consulta_con_chunks(sql_items_base, "C_ARTICULO", lista_articulos, 999, connection, where_clause_base="AND TIPO_CLASIF IN (4, 14)")
        if df_items.empty: return pd.DataFrame()

        df_dms['mos_id'] = pd.to_numeric(df_dms['mos_id'], errors='coerce')
        df_ms['mos_id'] = pd.to_numeric(df_ms['mos_id'], errors='coerce')
        df_merged = pd.merge(df_dms, df_ms, on='mos_id')
        df_merged = pd.merge(df_merged, df_items[['c_articulo', 'c_temporada']], on='c_articulo', how='inner')

        df_merged['c_lote'] = pd.to_numeric(df_merged['c_lote'], errors='coerce')
        df_lotes['c_lote'] = pd.to_numeric(df_lotes['c_lote'], errors='coerce')
        lotes_cols_sel = ['c_lote', 'clave_externa', 'id_subvalle', 'd_lote']
        df_merged = pd.merge(df_merged, df_lotes[lotes_cols_sel], on='c_lote', how='left')
        
        if 'c_deposito' in df_merged.columns:
            df_depositos_sel = df_depositos[['c_deposito', 'd_deposito']].rename(columns={'d_deposito': 'd_dorigen'})
            df_merged['c_deposito'] = pd.to_numeric(df_merged['c_deposito'], errors='coerce')
            df_depositos_sel['c_deposito'] = pd.to_numeric(df_depositos_sel['c_deposito'], errors='coerce')
            df_merged = pd.merge(df_merged, df_depositos_sel, on='c_deposito', how='left')
        else:
            df_merged['d_dorigen'] = None

        df_composicion = pd.DataFrame()
        df_composicion['C_LOTE'] = df_merged['c_lote']
        df_composicion['C_VARIEDAD_INV'] = df_merged['c_temporada']
        df_composicion['C_PERIODO'] = df_merged['cosecha']
        df_composicion['ID_SUBVALLE'] = df_merged['id_subvalle']
        df_composicion['CANTIDAD'] = df_merged['q_articulo']
        df_composicion['CLAVE_EXT_LOTE'] = df_merged['clave_externa']
        df_composicion['MOS_ID'] = df_merged['mos_id']
        df_composicion['ID'] = df_merged['dms_id']
        df_composicion['C_TIPO_COMPRO'] = df_merged['c_tipo_compro']
        df_composicion['F_MOVIMIENTO'] = df_merged['f_movimiento']
        df_composicion['D_LOTE'] = df_merged.get('d_lote')
        df_composicion['C_DORIGEN'] = df_merged.get('c_deposito')
        df_composicion['D_DORIGEN'] = df_merged.get('d_dorigen')
        df_composicion['ORIGEN'] = 'Ajuste Inv.'
        
        for col in ['C_LOTE_ORIGEN', 'PORCENTAJE_SI', 'CIU_NUMERO', 'NRO_INSCRIPCION', 'COD_CUARTEL', 'CUARTEL_LOG', 'C_DDESTINO', 'D_DDESTINO']:
            df_composicion[col] = None

        yield "Enriqueciendo ajustes con datos de órdenes de trabajo..."
        df_composicion = _enriquecer_con_ordenes_trabajo(df_composicion, engine)

        for col_lower in ['c_lote', 'mos_id', 'id', 'c_periodo', 'c_tipo_compro', 'c_dorigen']:
            col_upper = col_lower.upper()
            if col_upper in df_composicion.columns:
                 try: df_composicion[col_upper] = pd.to_numeric(df_composicion[col_upper], errors='coerce').astype('Int64')
                 except Exception:
                      try: df_composicion[col_upper] = pd.to_numeric(df_composicion[col_upper], errors='coerce').astype('Float64')
                      except Exception as e: yield f"Advertencia Ajuste Inv: No se pudo convertir {col_lower}: {e}"
        
        if 'F_MOVIMIENTO' in df_composicion.columns: df_composicion['F_MOVIMIENTO'] = pd.to_datetime(df_composicion['F_MOVIMIENTO'], errors='coerce')
        string_cols_lower = ['c_variedad_inv', 'id_subvalle', 'clave_ext_lote', 'd_lote', 'origen', 'd_dorigen'] + [c.lower() for c in COLUMNAS_OT]
        for col_lower in string_cols_lower:
             col_upper = col_lower.upper()
             if col_upper in df_composicion.columns: df_composicion[col_upper] = df_composicion[col_upper].astype(pd.StringDtype())
        
        if 'D_DORIGEN' in df_composicion.columns:
            df_composicion['D_DORIGEN'] = df_composicion['D_DORIGEN'].fillna('').str.slice(0, MAX_LEN_D_DEPOSITO)

        if 'CANTIDAD' in df_composicion.columns: df_composicion['CANTIDAD'] = df_composicion['CANTIDAD'].astype(float)

        final_order = ['C_LOTE', 'C_VARIEDAD_INV', 'C_PERIODO', 'ID_SUBVALLE', 'CANTIDAD', 'CLAVE_EXT_LOTE', 'MOS_ID', 'ID', 'C_TIPO_COMPRO', 'F_MOVIMIENTO', 'C_LOTE_ORIGEN', 'PORCENTAJE_SI', 'CIU_NUMERO', 'NRO_INSCRIPCION', 'COD_CUARTEL', 'CUARTEL_LOG', 'D_LOTE', 'C_DORIGEN', 'D_DORIGEN', 'C_DDESTINO', 'D_DDESTINO', 'ORIGEN'] + COLUMNAS_OT
        for col in final_order:
            if col not in df_composicion.columns: df_composicion[col] = None
        df_composicion = df_composicion.reindex(columns=final_order)
        return df_composicion

def procesar_transformaciones(engine: sqlalchemy.engine.Engine, fecha_desde_str: str, fecha_fin_str: str, db_user: str, df_lotes: pd.DataFrame, df_depositos: pd.DataFrame):
    yield "\n--- Iniciando Procesamiento de Transformaciones (Tipos 43, 30, 46) ---"
    target_table = 'APX_TRAZA_DETALLE'
    tipos_transformacion = [43, 30, 46]

    with engine.connect() as connection:
        sql_movim_stock = f"""SELECT ID, F_MOVIMIENTO, C_TIPO_COMPRO FROM MOVIM_STOCK WHERE C_TIPO_COMPRO IN ({','.join(map(str, tipos_transformacion))}) AND F_MOVIMIENTO >= TO_DATE(:fecha_inicio, 'YYYY-MM-DD') AND F_MOVIMIENTO < TO_DATE(:fecha_fin, 'YYYY-MM-DD') + 1 ORDER BY F_MOVIMIENTO ASC, ID ASC"""
        df_movim_transform = pd.read_sql(text(sql_movim_stock), connection, params={'fecha_inicio': fecha_desde_str, 'fecha_fin': fecha_fin_str})
        if df_movim_transform.empty:
            yield "No se encontraron transformaciones en el período."
            return pd.DataFrame(), pd.DataFrame()
        
        df_movim_transform.columns = df_movim_transform.columns.str.lower()
        df_movim_transform = df_movim_transform.rename(columns={'id': 'mos_id'}, errors='ignore')
        if 'mos_id' not in df_movim_transform.columns:
            yield "Error Crítico: 'mos_id' no encontrado."
            return pd.DataFrame(), pd.DataFrame()

        lista_mos_id_transform = df_movim_transform['mos_id'].dropna().unique().tolist()
        sql_dms_base = "SELECT ID, MOS_ID, C_LOTE, Q_ARTICULO, C_DEPOSITO FROM DET_MOV_STOCK"
        df_dms_transform = ejecutar_consulta_con_chunks(sql_dms_base, "MOS_ID", lista_mos_id_transform, 999, connection)
        df_dms_transform = df_dms_transform.rename(columns={'id': 'dms_id', 'c_lote': 'c_lote_destino', 'q_articulo': 'q_articulo_destino', 'c_deposito': 'c_deposito_destino'}, errors='ignore')

        sql_dpc_base = "SELECT DMS_ID, MOS_ID, ID, C_LOTE, Q_ARTIC_COMP, C_DEPOSITO FROM DET_PROD_COMP"
        df_dpc_transform = ejecutar_consulta_con_chunks(sql_dpc_base, "MOS_ID", lista_mos_id_transform, 999, connection)
        df_dpc_transform = df_dpc_transform.rename(columns={'id': 'dpc_id', 'c_lote': 'c_lote_origen', 'q_artic_comp': 'q_origen_usada', 'c_deposito': 'c_deposito_origen'}, errors='ignore')

        df_transform_base = pd.merge(df_dms_transform, df_dpc_transform, on=['mos_id', 'dms_id'], how='inner', suffixes=('_dest', '_orig'))
        df_transform_base = pd.merge(df_transform_base, df_movim_transform, on='mos_id', how='left')
        
        for col in ['c_lote_destino', 'c_lote_origen', 'mos_id', 'dms_id', 'dpc_id']:
            df_transform_base[col] = pd.to_numeric(df_transform_base[col], errors='coerce')
        
        df_transform_pendientes = df_transform_base[df_transform_base['c_lote_destino'] != df_transform_base['c_lote_origen']].copy()
        
        df_final_acumulado = pd.DataFrame()
        df_reporte_faltantes_final = pd.DataFrame()
        
        # --- CAMBIO: Bucle dinámico con mecanismos de seguridad ---
        iteracion_actual = 0
        max_iteraciones_seguridad = 30 

        while not df_transform_pendientes.empty:
            iteracion_actual += 1
            yield f"\n--- Iteración de Transformaciones {iteracion_actual} ---"

            pendientes_antes = len(df_transform_pendientes)
            
            if iteracion_actual > max_iteraciones_seguridad:
                yield f"ADVERTENCIA: Se alcanzó el número máximo de iteraciones de seguridad ({max_iteraciones_seguridad}). Saliendo del bucle."
                df_reporte_faltantes_final = df_transform_pendientes.copy()
                break
            
            lotes_origen_necesarios = df_transform_pendientes['c_lote_origen'].dropna().unique()
            
            # Usamos el nombre completo de la tabla para asegurar que lea lo que ya se insertó en la misma sesión
            sql_composicion_select = f"""SELECT C_LOTE, C_VARIEDAD_INV, C_PERIODO, ID_SUBVALLE, CANTIDAD, CLAVE_EXT_LOTE, NRO_INSCRIPCION, COD_CUARTEL, CUARTEL_LOG, CIU_NUMERO FROM {db_user}.{target_table}"""
            df_composicion_origen_actual = ejecutar_consulta_con_chunks(sql_composicion_select, "C_LOTE", lotes_origen_necesarios.tolist(), 999, connection)
            lotes_origen_encontrados = pd.to_numeric(df_composicion_origen_actual['c_lote'], errors='coerce').dropna().unique()

            df_procesables_ahora = df_transform_pendientes[df_transform_pendientes['c_lote_origen'].isin(lotes_origen_encontrados)]
            
            if df_procesables_ahora.empty:
                yield "No se pudieron procesar más transformaciones en esta iteración (Lotes origen no encontrados). Saliendo del bucle."
                df_reporte_faltantes_final = df_transform_pendientes.copy()
                break

            yield f"Se procesarán {len(df_procesables_ahora['mos_id'].unique())} transformaciones en esta iteración."

            df_composicion_origen_actual = df_composicion_origen_actual.rename(columns={'c_lote': 'c_lote_origen_comp', 'cantidad': 'cantidad_componente_origen'})
            df_composicion_origen_actual['c_lote_origen_comp'] = pd.to_numeric(df_composicion_origen_actual['c_lote_origen_comp'], errors='coerce')

            df_calculo = pd.merge(df_procesables_ahora, df_composicion_origen_actual, left_on='c_lote_origen', right_on='c_lote_origen_comp', how='left')
            df_calculo['cantidad_componente_origen'] = pd.to_numeric(df_calculo['cantidad_componente_origen'], errors='coerce').fillna(0)
            
            df_calculo['total_lote_origen'] = df_calculo.groupby('c_lote_origen')['cantidad_componente_origen'].transform('sum')
            df_calculo['total_lote_origen'] = pd.to_numeric(df_calculo['total_lote_origen'], errors='coerce').fillna(1).replace(0, 1)
            df_calculo['q_origen_usada'] = pd.to_numeric(df_calculo['q_origen_usada'], errors='coerce').fillna(0)
            df_calculo['cantidad_transferida'] = np.round((df_calculo['cantidad_componente_origen'] / df_calculo['total_lote_origen']) * df_calculo['q_origen_usada'], 5)
            
            df_calculo_filtrado = df_calculo[df_calculo['cantidad_transferida'] > 1e-9].copy()
            if df_calculo_filtrado.empty:
                ids_procesados = df_procesables_ahora['mos_id'].unique()
                df_transform_pendientes = df_transform_pendientes[~df_transform_pendientes['mos_id'].isin(ids_procesados)]
                yield f"Ningún componente resultó en transferencia de cantidad > 0. Quedan {len(df_transform_pendientes['mos_id'].unique())} transformaciones pendientes."
                continue

            cols_a_seleccionar = ['c_lote_destino', 'c_variedad_inv', 'c_periodo', 'id_subvalle', 'cantidad_transferida', 'clave_ext_lote', 'mos_id', 'dms_id', 'c_tipo_compro', 'f_movimiento', 'c_lote_origen', 'ciu_numero', 'nro_inscripcion', 'cod_cuartel', 'cuartel_log', 'c_deposito_origen', 'c_deposito_destino']
            cols_existentes = [col for col in cols_a_seleccionar if col in df_calculo_filtrado.columns]
            df_nuevas_composiciones = df_calculo_filtrado[cols_existentes].copy()
            df_lotes_min = df_lotes[['c_lote', 'd_lote']].rename(columns={'c_lote':'c_lote_destino', 'd_lote':'d_lote_destino'})
            df_lotes_min['c_lote_destino'] = pd.to_numeric(df_lotes_min['c_lote_destino'], errors='coerce')
            df_nuevas_composiciones['c_lote_destino'] = pd.to_numeric(df_nuevas_composiciones['c_lote_destino'], errors='coerce')
            df_nuevas_composiciones = pd.merge(df_nuevas_composiciones, df_lotes_min, on='c_lote_destino', how='left')
            df_depositos_orig = df_depositos[['c_deposito', 'd_deposito']].rename(columns={'c_deposito':'c_deposito_origen', 'd_deposito':'d_dorigen'})
            df_depositos_orig['c_deposito_origen'] = pd.to_numeric(df_depositos_orig['c_deposito_origen'], errors='coerce')
            df_nuevas_composiciones['c_deposito_origen'] = pd.to_numeric(df_nuevas_composiciones['c_deposito_origen'], errors='coerce')
            df_nuevas_composiciones = pd.merge(df_nuevas_composiciones, df_depositos_orig, on='c_deposito_origen', how='left')
            df_depositos_dest = df_depositos[['c_deposito', 'd_deposito']].rename(columns={'c_deposito':'c_deposito_destino', 'd_deposito':'d_ddestino'})
            df_depositos_dest['c_deposito_destino'] = pd.to_numeric(df_depositos_dest['c_deposito_destino'], errors='coerce')
            df_nuevas_composiciones['c_deposito_destino'] = pd.to_numeric(df_nuevas_composiciones['c_deposito_destino'], errors='coerce')
            df_nuevas_composiciones = pd.merge(df_nuevas_composiciones, df_depositos_dest, on='c_deposito_destino', how='left')
            df_nuevas_composiciones = df_nuevas_composiciones.rename(columns={'c_lote_destino': 'C_LOTE', 'cantidad_transferida': 'CANTIDAD', 'dms_id': 'ID', 'clave_ext_lote': 'CLAVE_EXT_LOTE', 'd_lote_destino': 'D_LOTE', 'c_deposito_origen': 'C_DORIGEN', 'c_deposito_destino': 'C_DDESTINO', 'c_variedad_inv': 'C_VARIEDAD_INV', 'c_periodo': 'C_PERIODO', 'id_subvalle': 'ID_SUBVALLE', 'mos_id': 'MOS_ID', 'c_tipo_compro': 'C_TIPO_COMPRO', 'f_movimiento': 'F_MOVIMIENTO', 'c_lote_origen': 'C_LOTE_ORIGEN', 'ciu_numero': 'CIU_NUMERO', 'nro_inscripcion': 'NRO_INSCRIPCION', 'cod_cuartel': 'COD_CUARTEL', 'cuartel_log': 'CUARTEL_LOG', 'd_dorigen': 'D_DORIGEN', 'd_ddestino': 'D_DDESTINO'})
            df_nuevas_composiciones['PORCENTAJE_SI'] = None
            origen_map = {43: 'Mezcla', 30: 'Reclasificacion', 46: 'Borras'}
            df_nuevas_composiciones['ORIGEN'] = df_nuevas_composiciones['C_TIPO_COMPRO'].map(origen_map).fillna('Transformacion')
            
            grouping_keys_upper = ['C_LOTE', 'C_VARIEDAD_INV', 'C_PERIODO', 'ID_SUBVALLE', 'CLAVE_EXT_LOTE', 'MOS_ID', 'ID', 'C_TIPO_COMPRO', 'F_MOVIMIENTO', 'C_LOTE_ORIGEN', 'PORCENTAJE_SI', 'CIU_NUMERO', 'NRO_INSCRIPCION', 'COD_CUARTEL', 'CUARTEL_LOG', 'D_LOTE', 'C_DORIGEN', 'D_DORIGEN', 'C_DDESTINO', 'D_DDESTINO', 'ORIGEN']
            for key in grouping_keys_upper:
                if key not in df_nuevas_composiciones.columns: df_nuevas_composiciones[key] = None
                if pd.api.types.is_datetime64_any_dtype(df_nuevas_composiciones[key]): df_nuevas_composiciones[key] = df_nuevas_composiciones[key].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('N/A')
                else: df_nuevas_composiciones[key] = df_nuevas_composiciones[key].apply(lambda x: 'N/A' if pd.isna(x) else str(x))
            df_nuevas_composiciones['CANTIDAD'] = pd.to_numeric(df_nuevas_composiciones['CANTIDAD'], errors='coerce').fillna(0)
            df_final_iteracion = df_nuevas_composiciones.groupby(grouping_keys_upper, dropna=False).agg(CANTIDAD=('CANTIDAD', 'sum')).reset_index().replace('N/A', None)
            
            if not df_final_iteracion.empty:
                yield f"Enriqueciendo transformación (iteración {iteracion_actual}) con datos de OT..."
                df_final_iteracion = _enriquecer_con_ordenes_trabajo(df_final_iteracion, engine)

                for col_upper in ['C_LOTE', 'MOS_ID', 'ID', 'CIU_NUMERO', 'C_PERIODO', 'C_TIPO_COMPRO', 'COD_CUARTEL', 'C_LOTE_ORIGEN', 'PORCENTAJE_SI', 'C_DORIGEN', 'C_DDESTINO']:
                    if col_upper in df_final_iteracion.columns:
                        try: df_final_iteracion[col_upper] = pd.to_numeric(df_final_iteracion[col_upper], errors='coerce').astype('Int64')
                        except Exception:
                            try: df_final_iteracion[col_upper] = pd.to_numeric(df_final_iteracion[col_upper], errors='coerce').astype('Float64')
                            except Exception as e: yield f"Advertencia Transform: No se pudo convertir {col_upper}: {e}"
                if 'F_MOVIMIENTO' in df_final_iteracion.columns: df_final_iteracion['F_MOVIMIENTO'] = pd.to_datetime(df_final_iteracion['F_MOVIMIENTO'], errors='coerce')
                
                string_cols_upper = ['C_VARIEDAD_INV', 'ID_SUBVALLE', 'CLAVE_EXT_LOTE', 'NRO_INSCRIPCION', 'CUARTEL_LOG', 'D_LOTE', 'ORIGEN', 'D_DORIGEN', 'D_DDESTINO'] + COLUMNAS_OT
                for col_upper in string_cols_upper:
                    if col_upper in df_final_iteracion.columns: df_final_iteracion[col_upper] = df_final_iteracion[col_upper].astype(pd.StringDtype())

                if 'D_DORIGEN' in df_final_iteracion.columns: df_final_iteracion['D_DORIGEN'] = df_final_iteracion['D_DORIGEN'].fillna('').str.slice(0, MAX_LEN_D_DEPOSITO)
                if 'D_DDESTINO' in df_final_iteracion.columns: df_final_iteracion['D_DDESTINO'] = df_final_iteracion['D_DDESTINO'].fillna('').str.slice(0, MAX_LEN_D_DEPOSITO)
                if 'CANTIDAD' in df_final_iteracion.columns: df_final_iteracion['CANTIDAD'] = df_final_iteracion['CANTIDAD'].astype(float)
            
                final_order = ['C_LOTE', 'C_VARIEDAD_INV', 'C_PERIODO', 'ID_SUBVALLE', 'CANTIDAD', 'CLAVE_EXT_LOTE', 'MOS_ID', 'ID', 'C_TIPO_COMPRO', 'F_MOVIMIENTO', 'C_LOTE_ORIGEN', 'PORCENTAJE_SI', 'CIU_NUMERO', 'NRO_INSCRIPCION', 'COD_CUARTEL', 'CUARTEL_LOG', 'D_LOTE', 'C_DORIGEN', 'D_DORIGEN', 'C_DDESTINO', 'D_DDESTINO', 'ORIGEN'] + COLUMNAS_OT
                final_order_existing = [col for col in final_order if col in df_final_iteracion.columns]
                df_final_iteracion = df_final_iteracion.reindex(columns=final_order_existing)
                
                yield f"Guardando {len(df_final_iteracion)} nuevas composiciones en la DB..."
                dtype_map = {'C_LOTE': BigInteger, 'C_VARIEDAD_INV': String(50), 'C_PERIODO': Integer, 'ID_SUBVALLE': String(8), 'CANTIDAD': Numeric(precision=20, scale=5), 'CLAVE_EXT_LOTE': String(100), 'MOS_ID': Integer, 'ID': Integer, 'C_TIPO_COMPRO': Integer, 'F_MOVIMIENTO': DateTime, 'C_LOTE_ORIGEN': BigInteger, 'PORCENTAJE_SI': Numeric(precision=5, scale=2), 'CIU_NUMERO': BigInteger, 'NRO_INSCRIPCION': String(7), 'COD_CUARTEL': Integer, 'CUARTEL_LOG': String(6), 'D_LOTE': String(54), 'C_DORIGEN': Integer, 'D_DORIGEN': String(MAX_LEN_D_DEPOSITO), 'C_DDESTINO': Integer, 'D_DDESTINO': String(MAX_LEN_D_DEPOSITO), 'ORIGEN': String(20)}
                dtype_map.update({col: String(255) for col in COLUMNAS_OT if 'CANT' not in col})
                dtype_map['CANT_ART_DESTINO'] = Numeric(precision=20, scale=5)
                dtype_map['CANT_ART_ORIGEN'] = Numeric(precision=20, scale=5)
                dtype_map_final = {k: v for k, v in dtype_map.items() if k in df_final_iteracion.columns}
                try:
                    df_final_iteracion.to_sql(name=target_table, con=engine, if_exists='append', index=False, dtype=dtype_map_final, chunksize=1000)
                    yield f"¡Éxito! Datos de la iteración guardados."
                    df_final_acumulado = pd.concat([df_final_acumulado, df_final_iteracion], ignore_index=True)
                except Exception as e_sql:
                    yield f"\n--- ERROR AL GUARDAR TRANSFORMACIONES EN BASE DE DATOS (Iteración {iteracion_actual}) ---"
                    yield f"Error: {e_sql}"

            ids_procesados = df_procesables_ahora['mos_id'].unique()
            df_transform_pendientes = df_transform_pendientes[~df_transform_pendientes['mos_id'].isin(ids_procesados)]
            
            pendientes_despues = len(df_transform_pendientes)
            yield f"Quedan {pendientes_despues} transformaciones pendientes."
            
            if pendientes_despues == pendientes_antes:
                yield "ADVERTENCIA: No se pudo procesar ninguna transformación adicional en esta iteración. Saliendo del bucle para evitar un ciclo infinito."
                df_reporte_faltantes_final = df_transform_pendientes.copy()
                break
        
        if df_transform_pendientes.empty:
            yield "¡Éxito! Todas las transformaciones fueron procesadas."

    if not df_reporte_faltantes_final.empty:
        df_reporte_faltantes_final = df_reporte_faltantes_final[['mos_id', 'dms_id', 'c_lote_origen', 'c_lote_destino', 'f_movimiento']].drop_duplicates()

    yield "--- Fin Procesamiento de Transformaciones ---"
    return df_final_acumulado, df_reporte_faltantes_final

def procesar_destinos_finales(engine: sqlalchemy.engine.Engine, db_user: str, lotes_con_composicion: list):
    yield "\n--- Iniciando Procesamiento de Destinos Finales ---"
    destino_final_table = f"{db_user}.APX_TRAZA_DESTINO_FINAL"
    
    with engine.connect() as connection:
        try:
            yield f"Limpiando la tabla de destinos: {destino_final_table}..."
            delete_stmt = text(f"DELETE FROM {destino_final_table}")
            connection.execute(delete_stmt)
            connection.commit()
        except Exception as e_delete:
            if "table or view does not exist" in str(e_delete).lower():
                yield f"La tabla {destino_final_table} no existe. Se creará automáticamente."
                connection.rollback()
            else:
                yield f"--- ADVERTENCIA AL BORRAR DATOS DE {destino_final_table} ---"
                yield f"Error: {e_delete}"
                connection.rollback()

        if not lotes_con_composicion:
            yield "No hay lotes para buscar su destino."
            return

        sql_dpc_select_part = "SELECT dpc.C_LOTE, dpc.Q_ARTIC_COMP, ms.ID as MOS_ID_DESTINO, ms.F_MOVIMIENTO, ms.C_TIPO_COMPRO FROM DET_PROD_COMP dpc JOIN MOVIM_STOCK ms ON dpc.MOS_ID = ms.ID"
        where_dpc = "AND ms.C_TIPO_COMPRO IN (41, 44)"
        df_dpc_destinos = ejecutar_consulta_con_chunks(sql_dpc_select_part, "dpc.C_LOTE", lotes_con_composicion, 999, connection, where_clause_base=where_dpc)
        
        df_final_dpc = pd.DataFrame()
        if not df_dpc_destinos.empty:
            destino_map_dpc = {41: 'PRODUCCION', 44: 'CONCENTRACION'}
            df_dpc_destinos['tipo_destino'] = df_dpc_destinos['c_tipo_compro'].map(destino_map_dpc)
            df_final_dpc = df_dpc_destinos.rename(columns={'c_lote': 'c_lote', 'q_artic_comp': 'cantidad_usada', 'f_movimiento': 'f_movimiento_destino'})[['c_lote', 'tipo_destino', 'cantidad_usada', 'f_movimiento_destino', 'mos_id_destino']]
            yield f"Se encontraron {len(df_final_dpc)} destinos en Producción/Concentración."

        sql_dfv_select_part = "SELECT dfv.C_LOTE_STOCK, dfv.Q_ARTICULO, fv.ID as FAC_ID_DESTINO, fv.F_FACTURA, fv.C_TIPO_COMPRO FROM DET_FAC_VEN dfv JOIN FACTURA_VENTAS fv ON dfv.FAC_ID = fv.ID"
        where_dfv = "AND fv.C_TIPO_COMPRO = 3"
        df_dfv_destinos = ejecutar_consulta_con_chunks(sql_dfv_select_part, "dfv.C_LOTE_STOCK", lotes_con_composicion, 999, connection, where_clause_base=where_dfv)

        df_final_dfv = pd.DataFrame()
        if not df_dfv_destinos.empty:
            df_dfv_destinos['tipo_destino'] = 'DESPACHADO'
            df_final_dfv = df_dfv_destinos.rename(columns={'c_lote_stock': 'c_lote', 'q_articulo': 'cantidad_usada', 'f_factura': 'f_movimiento_destino', 'fac_id_destino': 'mos_id_destino'})[['c_lote', 'tipo_destino', 'cantidad_usada', 'f_movimiento_destino', 'mos_id_destino']]
            yield f"Se encontraron {len(df_final_dfv)} destinos en Despachos."

        df_destinos_consolidados = pd.concat([df_final_dpc, df_final_dfv], ignore_index=True)
        if df_destinos_consolidados.empty:
            yield "No se encontró ningún destino final para los lotes procesados."
            return
            
        df_destinos_consolidados.columns = [col.upper() for col in df_destinos_consolidados.columns]
        
        yield f"\nIntentando guardar {len(df_destinos_consolidados)} registros de destinos finales en la tabla {destino_final_table}..."
        dtype_map_final = {'C_LOTE': BigInteger, 'TIPO_DESTINO': String(20), 'CANTIDAD_USADA': Numeric(precision=20, scale=5), 'F_MOVIMIENTO_DESTINO': DateTime, 'MOS_ID_DESTINO': Integer}
        try:
            df_destinos_consolidados.to_sql(name=destino_final_table.split('.')[1], con=engine, if_exists='append', index=False, dtype=dtype_map_final, chunksize=1000)
            yield f"¡Éxito! Datos de destinos finales guardados."
        except Exception as e_sql:
            yield f"\n--- ERROR AL GUARDAR DESTINOS FINALES EN BASE DE DATOS ---"
            yield f"Error: {e_sql}"


def ejecutar_proceso_completo(fecha_inicio_str: str, fecha_fin_str: str):
    cred_file_path = Path(r"C:\projectdj\acceso.pwd")
    tns_alias = "CGGBD1"
    tns_admin_dir = r"C:\oracle\instantclient_21_8\network\admin"
    db_user = None
    db_pass = None
    ORACLE_IN_CLAUSE_LIMIT = 999

    yield f"Intentando leer credenciales desde: {cred_file_path}"
    try:
        if not cred_file_path.is_file(): raise FileNotFoundError(f"Archivo no encontrado: {cred_file_path}")
        with open(cred_file_path, 'r') as f: lines = f.readlines()
        if len(lines) < 2: raise ValueError("Archivo debe contener usuario y contraseña en líneas separadas.")
        db_user = lines[0].strip()
        db_pass = lines[1].strip()
        yield "Credenciales leídas desde el archivo."
    except Exception as file_err: 
        yield f"Error crítico al leer credenciales: {file_err}"
        return

    if not db_user or not db_pass:
        yield "Usuario o contraseña no encontrados en el archivo de credenciales."
        return

    dsn = f"oracle+oracledb://{db_user}:{db_pass}@{tns_alias}"
    engine = None
    yield "\nIniciando proceso de trazabilidad..."
    try:
        yield f"Intentando conectar a Oracle usando TNS Alias '{tns_alias}'..."
        try:
             engine = create_engine(dsn, connect_args={'config_dir': tns_admin_dir})
             with engine.connect() as connection_test:
                 yield "¡Conexión a la base de datos exitosa!"
        except Exception as conn_err: 
            yield f"Error de conexión: {conn_err}"
            return

        target_table_name_base = "APX_TRAZA_DETALLE"
        full_target_table_name = f"{db_user}.{target_table_name_base.upper()}"

        with engine.connect() as connection:
            try:
                yield f"\nIntentando borrar todos los registros de la tabla {full_target_table_name}..."
                delete_stmt = text(f"DELETE FROM {full_target_table_name}")
                result = connection.execute(delete_stmt)
                connection.commit()
                yield f"¡Éxito! {result.rowcount} registros borrados de {full_target_table_name}."
            except Exception as e_delete:
                yield f"--- ERROR AL BORRAR DATOS DE {full_target_table_name} ---"
                yield f"Error detallado: {e_delete}"
                yield "Por favor, verifique los permisos del usuario o la existencia de la tabla."
                yield "El script continuará, pero los resultados pueden ser acumulativos si el borrado falló."
                try: connection.rollback()
                except: pass

        yield f"\nProcesando datos entre {fecha_inicio_str} y {fecha_fin_str}"

        yield "\nExtrayendo datos maestros comunes..."
        with engine.connect() as connection:
            sql_lotes_stock_all = "SELECT C_LOTE, CLAVE_EXTERNA, ID_SUBVALLE, D_LOTE FROM LOTES_STOCK"
            df_lotes_maestro = pd.read_sql(text(sql_lotes_stock_all), connection)
            df_lotes_maestro.columns = df_lotes_maestro.columns.str.lower()
            sql_cuartel_logico_all = "SELECT CUART_COD, CODIGO, ID_SUBVALLE FROM CUARTEL_LOGICO"
            df_cl_maestro = pd.read_sql(text(sql_cuartel_logico_all), connection)
            df_cl_maestro.columns = df_cl_maestro.columns.str.lower()
            sql_depositos_all = "SELECT C_DEPOSITO, D_DEPOSITO FROM DEPOSITOS"
            df_depositos_maestro = pd.read_sql(text(sql_depositos_all), connection)
            df_depositos_maestro.columns = df_depositos_maestro.columns.str.lower()
        yield "Datos maestros extraídos."
        
        df_compras = yield from procesar_compras(engine, fecha_inicio_str, fecha_fin_str, db_user, df_lotes_maestro.copy(), df_depositos_maestro.copy())
        if not df_compras.empty:
            df_compras.to_sql(name=target_table_name_base, con=engine, if_exists='append', index=False, chunksize=1000)
            yield f"¡Éxito! {len(df_compras)} registros de compras guardados."
        
        yield "\n--- Iniciando Procesamiento de Descubes (Tipo 28) ---"
        with engine.connect() as connection:
            sql_movim_stock_desc = f"""SELECT ID, F_MOVIMIENTO, C_TIPO_COMPRO FROM MOVIM_STOCK WHERE C_TIPO_COMPRO = 28 AND F_MOVIMIENTO >= TO_DATE(:f_ini, 'YYYY-MM-DD') AND F_MOVIMIENTO < TO_DATE(:f_fin, 'YYYY-MM-DD') + 1"""
            df_movim_stock_desc = pd.read_sql(text(sql_movim_stock_desc), connection, params={'f_ini': fecha_inicio_str, 'f_fin': fecha_fin_str})
            if not df_movim_stock_desc.empty:
                id_col_name_ms = 'id' if 'id' in df_movim_stock_desc.columns else 'ID'
                lista_mos_id_desc = df_movim_stock_desc[id_col_name_ms].dropna().unique().tolist()
                sql_dms_base = "SELECT ID, MOS_ID, C_LOTE, Q_ARTICULO FROM DET_MOV_STOCK"
                df_dms_desc = ejecutar_consulta_con_chunks(sql_dms_base, "MOS_ID", lista_mos_id_desc, ORACLE_IN_CLAUSE_LIMIT, connection)
                sql_cd_base = "SELECT MOS_ID, COS_NUMERO, Q_KILOS, DEP_C_DEPOSITO FROM COSECHA_DEPOSITO"
                df_cd_desc = ejecutar_consulta_con_chunks(sql_cd_base, "MOS_ID", lista_mos_id_desc, ORACLE_IN_CLAUSE_LIMIT, connection)
                id_col_name_cd_cos = 'cos_numero' if 'cos_numero' in df_cd_desc.columns else 'COS_NUMERO'
                lista_cos_numero_desc = df_cd_desc[id_col_name_cd_cos].dropna().unique().tolist() if not df_cd_desc.empty and id_col_name_cd_cos in df_cd_desc.columns else []
                sql_cosecha_base = "SELECT NUMERO, C_VARIEDAD_INV, C_PERIODO, ID_SUBVALLE, VIÑ_NRO_INSCRIPCION FROM COSECHA"
                df_cosecha_desc = ejecutar_consulta_con_chunks(sql_cosecha_base, "NUMERO", lista_cos_numero_desc, ORACLE_IN_CLAUSE_LIMIT, connection)
                sql_cc_base = "SELECT CSC_NUMERO, CCU_COD_CUARTEL, CCU_CUART_LOG, ID_SUBVALLE FROM COSECHA_CUARTELES"
                df_cc_desc = ejecutar_consulta_con_chunks(sql_cc_base, "CSC_NUMERO", lista_cos_numero_desc, ORACLE_IN_CLAUSE_LIMIT, connection)
                datos_descubes_dict = {"movim_stock": df_movim_stock_desc, "det_mov_stock": df_dms_desc, "cosecha_deposito": df_cd_desc, "cosecha": df_cosecha_desc, "cosecha_cuarteles": df_cc_desc}
                df_composicion_descubes_real = procesar_descubes(datos_descubes_dict, df_lotes_maestro.copy(), df_cl_maestro.copy(), df_depositos_maestro.copy())
                if not df_composicion_descubes_real.empty:
                    yield "Enriqueciendo descubes con datos de órdenes de trabajo..."
                    df_composicion_descubes_real = _enriquecer_con_ordenes_trabajo(df_composicion_descubes_real, engine)
                    df_composicion_descubes_real.to_sql(name=target_table_name_base, con=engine, if_exists='append', index=False, chunksize=1000)
                    yield f"¡Éxito! {len(df_composicion_descubes_real)} registros de descubes guardados."
            else: yield "No hay movimientos de descube en el período para procesar."
            yield "--- Fin Procesamiento de Descubes ---"

        df_ajustes_result = yield from procesar_ajustes_inventario(engine, fecha_inicio_str, fecha_fin_str, db_user, df_lotes_maestro.copy(), df_depositos_maestro.copy())
        if not df_ajustes_result.empty:
            df_ajustes_result.to_sql(name=target_table_name_base, con=engine, if_exists='append', index=False, chunksize=1000)
            yield f"¡Éxito! {len(df_ajustes_result)} registros de ajustes guardados."

        df_transform_result, df_reporte_faltantes_transformaciones = yield from procesar_transformaciones(engine, fecha_inicio_str, fecha_fin_str, db_user, df_lotes_maestro.copy(), df_depositos_maestro.copy())

        if df_reporte_faltantes_transformaciones is not None and not df_reporte_faltantes_transformaciones.empty:
            nombre_reporte_faltantes = "reporte_lotes_origen_sin_composicion.csv"
            try:
                df_reporte_faltantes_transformaciones.to_csv(nombre_reporte_faltantes, index=False, encoding='utf-8-sig')
                yield f"\nSe generó un reporte de lotes origen sin composición: {nombre_reporte_faltantes}"
            except Exception as e_csv:
                yield f"\nError al guardar el reporte CSV '{nombre_reporte_faltantes}': {e_csv}"
        else:
            yield "\nNo se encontraron lotes origen sin composición durante las transformaciones."
            
        with engine.connect() as connection:
            lotes_con_composicion = pd.read_sql(text(f"SELECT DISTINCT C_LOTE FROM {full_target_table_name}"), connection)['c_lote'].tolist()
        yield from procesar_destinos_finales(engine, db_user, lotes_con_composicion)
        
    except sqlalchemy.exc.DatabaseError as db_err: yield f"\n--- ERROR DE BASE DE DATOS ---: {db_err}"
    except KeyError as key_err: yield f"\n--- ERROR DE CLAVE (KeyError) ---: {key_err}\n{traceback.format_exc()}"
    except Exception as e: yield f"\n--- ERROR INESPERADO ---: {e}\n{traceback.format_exc()}"
    finally:
        if engine: engine.dispose(); yield "\nConexión a base de datos cerrada."
        yield "Proceso finalizado."


if __name__ == "__main__":
    while True:
        fecha_inicio_str = input("Ingrese la fecha de inicio (YYYY-MM-DD): ")
        try:
            datetime.strptime(fecha_inicio_str, '%Y-%m-%d')
            break
        except ValueError:
            print("Formato de fecha incorrecto. Por favor, use YYYY-MM-DD.")

    while True:
        fecha_fin_str = input("Ingrese la fecha de fin (YYYY-MM-DD): ")
        try:
            datetime.strptime(fecha_fin_str, '%Y-%m-%d')
            if datetime.strptime(fecha_fin_str, '%Y-%m-%d') < datetime.strptime(fecha_inicio_str, '%Y-%m-%d'):
                print("Error: La fecha de fin no puede ser anterior a la fecha de inicio.")
            else:
                break
        except ValueError:
            print("Formato de fecha incorrecto. Por favor, use YYYY-MM-DD.")

    print("\n--- LOG DE EJECUCIÓN ---")
    for line in ejecutar_proceso_completo(fecha_inicio_str, fecha_fin_str):
        print(line)