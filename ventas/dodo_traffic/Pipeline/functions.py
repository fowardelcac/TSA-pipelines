import pandas as pd
import numpy as np
from sqlmodel import select, Session
from Pipeline.model import Vendedor, Presupuesto, Reserva, Oddo

# from Pipeline.model import Vendedor, Presupuesto, Reserva, Oddo
from sqlalchemy.exc import SQLAlchemyError
import unicodedata
from Pipeline.loggins_system import *


class ProcessData:
    @staticmethod
    def clean_str(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        for col in columns:
            if col in df.columns:
                # Primero hacer strip y upper solo en valores no nulos
                df[col] = df[col].apply(
                    lambda x: (
                        x.strip().upper() if pd.notna(x) and str(x).strip() else None
                    )
                )
        return df

    @staticmethod
    def clean_nan(value):
        return None if pd.isna(value) else value

    @staticmethod
    def quitar_acentos(texto: str) -> str:
        if not texto:
            return ""
        # Normaliza y elimina caracteres diacríticos
        texto = unicodedata.normalize("NFKD", texto)
        texto = "".join(c for c in texto if not unicodedata.combining(c))
        texto = texto.replace("ñ", "n").replace("Ñ", "N")
        return texto

    @staticmethod
    def process_data(
        df: pd.DataFrame,
        drop_col: str,
        rename_map: dict,
        date_cols: list[str],
        round_cols: list[str],
        output_path: str | None = None,
    ) -> pd.DataFrame:
        """Procesa un DataFrame de presupuestos o reservas."""

        # --- Eliminar columna innecesaria ---
        df = df.drop(columns=[drop_col], errors="ignore")

        # --- Renombrar columnas ---
        df = df.rename(columns=rename_map)

        # --- Limpiar strings ---
        df = ProcessData.clean_str(df, df.select_dtypes(include="object").columns)
        if "vendedor" in df.columns:
            df["vendedor"] = df["vendedor"].apply(ProcessData.quitar_acentos)
        # --- Fechas ---
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

        # --- Niños ---
        if "can_chd" in df.columns:
            df["can_chd"] = (
                pd.to_numeric(df["can_chd"], errors="coerce").round(0).astype("Int64")
            )

        # --- Redondear numéricos ---
        for col in round_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

        # --- Duplicados y nulos ---
        duplicated_rows = df[df.duplicated(subset=["reserva"], keep="first")].copy()
        df.drop_duplicates(subset=["reserva"], keep="first", inplace=True)

        missing_file_rows = df[df["reserva"].isna()].copy()
        df.dropna(subset=["reserva"], inplace=True)

        # --- Guardar eliminadas ---
        removed_rows = pd.concat([duplicated_rows, missing_file_rows]).drop_duplicates()
        if not removed_rows.empty and output_path:
            removed_rows.to_excel(output_path, index=False)

        df = df.replace({np.nan: None})
        return df

    # --- Métodos convenientes que usan la genérica ---
    @staticmethod
    def process_pres(df: pd.DataFrame, output_path: str | None = None) -> pd.DataFrame:
        return ProcessData.process_data(
            df=df,
            drop_col="Idpresupu",
            rename_map={
                "Rva": "reserva",
                "Tiporva": "tipo_reserva",
                "Fec_mod": "ultima_modif",
                "Fec_rva": "fecha_reserva",
                "Fec_sal": "fecha_salida",
                "Nombreagencia_cod_agcia": "cliente",
                "Observ": "nombre_grupo",
                "Estado": "estado",
                "Can_adu": "can_adu",
                "Can_chd": "can_chd",
                "Moneda": "moneda",
                "Nombrevendedor_cod_vdor": "vendedor",
                "Total": "total",
                "costoConIva": "costo_final",
                "GananciaTotal": "ganancia",
                "Productos": "productos",
            },
            date_cols=["ultima_modif", "fecha_reserva", "fecha_salida"],
            round_cols=["total", "costo_final", "ganancia", "productos"],
            output_path=output_path,
        )

    @staticmethod
    def process_rva(df: pd.DataFrame, output_path: str | None = None) -> pd.DataFrame:
        return ProcessData.process_data(
            df=df,
            drop_col="Idreserva",
            rename_map={
                "Rva": "reserva",
                "Tiporva": "tipo_reserva",
                "Fec_mod": "ultima_modif",
                "Fec_rva": "fecha_reserva",
                "Fec_sal": "fecha_salida",
                "Fec_fin": "fecha_fin",
                "Nombreagencia_cod_agcia": "cliente",
                "Nombregrupo": "nombre_grupo",
                "Estado": "estado",
                "Can_adu": "can_adu",
                "Can_chd": "can_chd",
                "Moneda": "moneda",
                "Nombrevendedor_cod_vdor": "vendedor",
                "Total": "total",
                "gananciaTotal": "ganancia",
                "Tipocont": "eje_reservas_para",
                "Descripparame_productos": "descrip_productos",
            },
            date_cols=["ultima_modif", "fecha_reserva", "fecha_salida", "fecha_fin"],
            round_cols=["total", "ganancia"],
            output_path=output_path,
        )

    @staticmethod
    def process_oddo(df: pd.DataFrame):
        def limpiar_ganancia(valor):
            """Limpia y formatea el valor de ganancia_esperada"""
            if pd.isna(valor) or valor is None:
                return None

            # Convertir a string y limpiar
            valor_str = str(valor).strip()

            # Remover espacios no-break (\xa0) y convertir a espacio normal
            valor_str = valor_str.replace("\xa0", " ")

            # Si está vacío después de limpiar, retornar None
            if not valor_str:
                return None

            return valor_str

        df.drop("email_from", axis=1, inplace=True)
        new_cols: dict = {
            "name": "nombre_grupo",
            "user_id": "vendedor",
            "expected_revenue": "ganancia_esperada",
            "stage_id": "estado",
        }
        df = df.rename(columns=new_cols)
        df = ProcessData.clean_str(df, df.select_dtypes(include="object").columns)
        df["vendedor"] = df["vendedor"].apply(ProcessData.quitar_acentos)
        df["ganancia_esperada"] = df["ganancia_esperada"].apply(limpiar_ganancia)
        df = df.replace({np.nan: None})
        return df


class Loader:
    @staticmethod
    def create_vendor(nombre_completo: str, nombre: str, session: Session) -> None:
        new_vendedor = Vendedor(nombre_completo=nombre_completo, nombre=nombre)
        session.add(new_vendedor)
        session.flush()

    @staticmethod
    def upsert_reservas(
        press_f: pd.DataFrame,
        session: Session,
        logger: logging.Logger,
        tracker_reservas: ProcessTracker = ProcessTracker(),
    ):
        tracker_reservas.increment_processed()

        logger.info("Iniciando carga de reservas...\n")
        # Normalizamos: quitamos espacios y ponemos todo en mayúscula
        vendedores: dict = {
            v.nombre_completo.strip().upper(): v
            for v in session.exec(select(Vendedor)).all()
        }

        # 🔹 Cachear reservas existentes (clave: código reserva)
        reservas_existentes: dict = {
            r.reserva: r for r in session.exec(select(Reserva)).all()
        }

        for index, row in press_f.iterrows():
            try:

                vendedor_nombre = row.get("vendedor")
                vendedor_obj: Vendedor = vendedores.get(vendedor_nombre)

                if not vendedor_obj:
                    logger.warning(
                        f"  ⚠️  Vendedor '{vendedor_nombre}' no encontrado, saltando..."
                    )
                    tracker_reservas.add_error(
                        row.get("reserva"),
                        f"Vendedor '{vendedor_nombre}' no encontrado",
                    )
                    continue

                # Datos comunes
                data = dict(
                    reserva=ProcessData.clean_nan(row.get("reserva")),
                    tipo_reserva=ProcessData.clean_nan(row.get("tipo_reserva")),
                    ultima_modif=ProcessData.clean_nan(row.get("ultima_modif")),
                    fecha_reserva=ProcessData.clean_nan(row.get("fecha_reserva")),
                    fecha_salida=ProcessData.clean_nan(row.get("fecha_salida")),
                    fecha_fin=ProcessData.clean_nan(row.get("fecha_fin")),
                    cliente=ProcessData.clean_nan(row.get("cliente")),
                    vendedor_id=vendedor_obj.vendedor_id,
                    nombre_grupo=ProcessData.clean_nan(row.get("nombre_grupo")),
                    estado=ProcessData.clean_nan(row.get("estado")),
                    can_adu=ProcessData.clean_nan(row.get("can_adu")),
                    can_chd=ProcessData.clean_nan(row.get("can_chd")),
                    moneda=ProcessData.clean_nan(row.get("moneda")),
                    total=ProcessData.clean_nan(row.get("total")),
                    ganancia=ProcessData.clean_nan(row.get("ganancia")),
                    eje_reservas_para=ProcessData.clean_nan(
                        row.get("eje_reservas_para")
                    ),
                    descrip_productos=ProcessData.clean_nan(
                        row.get("descrip_productos")
                    ),
                )

                rva_key = data["reserva"]

                # 🔹 Si ya existe → actualizar
                if rva_key in reservas_existentes:
                    obj = reservas_existentes[rva_key]
                    cambios = {}
                    for k, v in data.items():
                        actual = getattr(obj, k)
                        if actual != v:
                            cambios[k] = (actual, v)
                            setattr(obj, k, v)

                    if cambios:
                        tracker_reservas.add_update(
                            row.get("reserva"), list(cambios.keys())
                        )
                        logger.info(
                            f"  🔄 Actualizado {rva_key}: {list(cambios.keys())}"
                        )
                # 🔹 Si no existe → crear nuevo
                else:
                    session.add(Reserva(**data))
                    logger.info(f"  📝 Creado nuevo: {rva_key}")
                    tracker_reservas.add_new(row.get("reserva"))

            except SQLAlchemyError as e:
                logger.error(f"  ❌ Error SQL en {row.get('reserva')}: {e}")
                tracker_reservas.add_error(row.get("reserva"), f"Error SQL: {e}")
                session.rollback()
            except Exception as e:
                logger.error(f"  ❌ Error SQL en {row.get('reserva')}: {e}")
                tracker_reservas.add_error(row.get("reserva"), row, f"Error SQL: {e}")
                session.rollback()

        # 🔹 Commit final
        session.commit()

        summary = tracker_reservas.get_summary()

        logger.info("Commit --Reservas-- final realizado.")
        logger.info("📊 RESUMEN FINAL DEL PROCESO RESERVAS")
        logger.info("=" * 60)
        logger.info(f"⏰ Duración total: {summary['duracion']}")
        logger.info(f"📁 Total procesadas: {summary['stats']['total_procesadas']}")
        logger.info(f"✨ Nuevos registros: {summary['stats']['nuevos']}")
        logger.info(f"📝 Registros actualizados: {summary['stats']['actualizados']}")
        logger.info(f"❌ Errores: {summary['stats']['errores']}")
        logger.info("=" * 60)
        logger.info("✅ PROCESO COMPLETADO")
        logger.info("=" * 60)

    @staticmethod
    def upsert_presupuestos(
        press_f: pd.DataFrame,
        session: Session,
        logger: logging.Logger,
        tracker_presup: ProcessTracker = ProcessTracker(),
    ):
        tracker_presup.increment_processed()
        logger.info("Iniciando carga de presupuestos...\n")

        # 🔹 1. Cachear todos los vendedores una sola vez
        vendedores: dict = {v.nombre: v for v in session.exec(select(Vendedor)).all()}

        # 🔹 2. Obtener presupuestos existentes una sola vez
        reservas_existentes: dict = {
            p.reserva: p for p in session.exec(select(Presupuesto)).all()
        }

        for index, row in press_f.iterrows():

            try:
                vendedor_nombre = row.get("vendedor")
                vendedor_obj: Vendedor = vendedores.get(vendedor_nombre)
                if not vendedor_obj:
                    logger.warning(
                        f"  ⚠️  Vendedor '{vendedor_nombre}' no encontrado, saltando..."
                    )
                    tracker_presup.add_error(
                        row.get("reserva"),
                        f"Vendedor '{vendedor_nombre}' no encontrado",
                    )
                    continue

                rva_key = row.get("reserva")
                data = dict(
                    reserva=ProcessData.clean_nan(row.get("reserva")),
                    tipo_reserva=ProcessData.clean_nan(row.get("tipo_reserva")),
                    ultima_modif=ProcessData.clean_nan(row.get("ultima_modif")),
                    fecha_reserva=ProcessData.clean_nan(row.get("fecha_reserva")),
                    fecha_salida=ProcessData.clean_nan(row.get("fecha_salida")),
                    cliente=ProcessData.clean_nan(row.get("cliente")),
                    vendedor_id=vendedor_obj.vendedor_id,
                    nombre_grupo=ProcessData.clean_nan(row.get("nombre_grupo")),
                    estado=ProcessData.clean_nan(row.get("estado")),
                    can_adu=ProcessData.clean_nan(row.get("can_adu")),
                    can_chd=ProcessData.clean_nan(row.get("can_chd")),
                    moneda=ProcessData.clean_nan(row.get("moneda")),
                    total=ProcessData.clean_nan(row.get("total")),
                    ganancia=ProcessData.clean_nan(row.get("ganancia")),
                    costo_final=ProcessData.clean_nan(row.get("costo_final")),
                    productos=ProcessData.clean_nan(row.get("productos")),
                )

                # 🔹 Si ya existe → actualizar
                if rva_key in reservas_existentes:
                    obj = reservas_existentes[rva_key]
                    cambios = {}
                    for k, v in data.items():
                        actual = getattr(obj, k)
                        if actual != v:
                            cambios[k] = (actual, v)
                            setattr(obj, k, v)

                    if cambios:
                        logger.info(
                            f"  🔄 Actualizado {rva_key}: {list(cambios.keys())}"
                        )
                        tracker_presup.add_update(
                            row.get("reserva"), list(cambios.keys())
                        )
                # 🔹 Si no existe → crear nuevo
                else:
                    session.add(Presupuesto(**data))
                    logger.info(f"  📝 Creado nuevo: {rva_key}")
                    tracker_presup.add_new(row.get("reserva"))

            except SQLAlchemyError as e:
                logger.error(f"  ❌ Error SQL en {row.get('reserva')}: {e}")
                tracker_presup.add_error(row.get("reserva"), f"Error SQL: {e}")
                session.rollback()
            except Exception as e:
                logger.error(f"  ❌ Error SQL en {row.get('reserva')}: {e}")
                tracker_presup.add_error(row.get("reserva"), f"Error SQL: {e}")
                session.rollback()

        # 🔹 Commit final
        session.commit()
        summary = tracker_presup.get_summary()

        logger.info("Commit --Presupuestos-- final realizado.")
        logger.info("📊 RESUMEN FINAL DEL PROCESO PRESUPUESTO")
        logger.info("=" * 60)
        logger.info(f"⏰ Duración total: {summary['duracion']}")
        logger.info(f"📁 Total procesadas: {summary['stats']['total_procesadas']}")
        logger.info(f"✨ Nuevos registros: {summary['stats']['nuevos']}")
        logger.info(f"📝 Registros actualizados: {summary['stats']['actualizados']}")
        logger.info(f"❌ Errores: {summary['stats']['errores']}")
        logger.info("=" * 60)
        logger.info("✅ PROCESO COMPLETADO")
        logger.info("=" * 60)

    @staticmethod
    def upsert_oddo(
        oddo_f: pd.DataFrame,
        session: Session,
        logger: logging.Logger,
        tracker_oddo: ProcessTracker = ProcessTracker(),
    ):
        tracker_oddo.increment_processed()
        logger.info("Iniciando carga de oddos...\n")
        # 🔹 1. Cachear todos los vendedores
        vendedores: dict = {
            v.nombre_completo: v for v in session.exec(select(Vendedor)).all()
        }

        # 🔹 2. Cachear Oddos existentes por nombre_grupo
        oddos_existentes: dict = {
            o.nombre_grupo: o for o in session.exec(select(Oddo)).all()
        }

        for index, row in oddo_f.iterrows():

            try:
                vendedor_nombre = row.get("vendedor")
                vendedor_obj: Vendedor = vendedores.get(vendedor_nombre)

                if not vendedor_obj:
                    logger.warning(
                        f"  ⚠️  Vendedor '{vendedor_nombre}' no encontrado, saltando..."
                    )
                    tracker_oddo.add_error(
                        row.get("nombre_grupo"),
                        f"Vendedor '{vendedor_nombre}' no encontrado",
                    )
                    continue

                grupo_key = row.get("nombre_grupo")

                # 🔹 Diccionario con todos los campos a actualizar/insertar
                data = {
                    "vendedor_id": vendedor_obj.vendedor_id,
                    "ganancia_esperada": row.get("ganancia_esperada"),
                    "estado": row.get("estado"),
                }

                # 🔹 Si ya existe → actualizar automáticamente
                if grupo_key in oddos_existentes:
                    obj = oddos_existentes[grupo_key]
                    cambios = {}
                    for k, v in data.items():
                        actual = getattr(obj, k)
                        if actual != v:
                            cambios[k] = (actual, v)
                            setattr(obj, k, v)

                    if cambios:
                        logger.info(
                            f"  🔄 Actualizado {grupo_key}: {list(cambios.keys())}"
                        )
                        tracker_oddo.add_update(
                            row.get("nombre_grupo"), list(cambios.keys())
                        )

                # 🔹 Si no existe → crear nuevo
                else:
                    new_oddo = Oddo(**data, nombre_grupo=grupo_key)
                    session.add(new_oddo)
                    logger.info(f"  📝 Creado nuevo: {grupo_key}")
                    tracker_oddo.add_new(row.get("nombre_grupo"))

            except Exception as e:
                logger.error(f"  ❌ Error SQL en {row.get('reserva')}: {e}")
                tracker_oddo.add_error(row.get("nombre_grupo"), f"Error SQL: {e}")
                session.rollback()
                continue

        # 🔹 Commit final
        session.commit()
        summary = tracker_oddo.get_summary()

        logger.info("Commit --Oddo-- final realizado.")
        logger.info("📊 RESUMEN FINAL DEL PROCESO ODDO")
        logger.info("=" * 60)
        logger.info(f"⏰ Duración total: {summary['duracion']}")
        logger.info(f"📁 Total procesadas: {summary['stats']['total_procesadas']}")
        logger.info(f"✨ Nuevos registros: {summary['stats']['nuevos']}")
        logger.info(f"📝 Registros actualizados: {summary['stats']['actualizados']}")
        logger.info(f"❌ Errores: {summary['stats']['errores']}")
        logger.info("=" * 60)
        logger.info("✅ PROCESO COMPLETADO")
        logger.info("=" * 60)
