from typing import Optional
from sqlmodel import SQLModel, Field
from datetime import date
from decimal import Decimal


class Vendedor(SQLModel, table=True):
    __tablename__ = "vendedores"
    vendedor_id: int | None = Field(default=None, primary_key=True)
    nombre_completo: str = Field(max_length=150)
    nombre: str = Field(max_length=50)


# Tabla presupuestos
class Presupuesto(SQLModel, table=True):
    __tablename__ = "presupuestos"
    reserva_id: int | None = Field(default=None, primary_key=True)
    reserva: Optional[str] = Field(max_length=6)
    tipo_reserva: Optional[str] = Field(max_length=4)
    ultima_modif: Optional[date]
    fecha_reserva: Optional[date]
    fecha_salida: Optional[date]
    cliente: Optional[str] = Field(max_length=255)
    nombre_grupo: Optional[str] = Field(max_length=255)
    estado: Optional[str] = Field(max_length=2)
    can_adu: Optional[int]
    can_chd: Optional[int]
    moneda: Optional[str] = Field(max_length=2)
    total: float
    costo_final: float
    ganancia: float
    productos: Optional[str] = Field(max_length=255)
    vendedor_id: int = Field(foreign_key="vendedores.vendedor_id")


# Tabla reservas
class Reserva(SQLModel, table=True):
    __tablename__ = "reservas"
    reserva_id: int | None = Field(default=None, primary_key=True)
    reserva: Optional[str] = Field(max_length=6)
    tipo_reserva: Optional[str] = Field(max_length=4)
    ultima_modif: Optional[date]
    fecha_reserva: Optional[date]
    fecha_salida: Optional[date]
    fecha_fin: Optional[date]
    cliente: Optional[str] = Field(max_length=255)
    nombre_grupo: Optional[str] = Field(max_length=255)
    estado: Optional[str] = Field(max_length=2)
    can_adu: Optional[int]
    can_chd: Optional[int]
    moneda: Optional[str] = Field(max_length=2)
    total: float
    ganancia: float
    eje_reservas_para: Optional[str] = Field(max_length=200)
    descrip_productos: Optional[str] = Field(max_length=255)
    vendedor_id: int = Field(foreign_key="vendedores.vendedor_id")


# Tabla oddo (CRM)
class Oddo(SQLModel, table=True):
    __tablename__ = "oddos"
    oddo_id: int | None = Field(default=None, primary_key=True)
    nombre_grupo: Optional[str] = Field(max_length=255)
    ganancia_esperada: Optional[str] = Field(max_length=100)
    estado: Optional[str] = Field(
        max_length=50,
        description="Enum: 'VIAJADO', 'Confirmado', 'Propuesta Enviada', 'Leads', "
        "'Stand by / pospuesto', '1º FUP', 'PAQUETES', '3º FUP', 'Bloqueado', "
        "'Cancelado', 'Armando Propuesta', '4º FUP', '2º FUP', '5º FUP'",
    )
    vendedor_id: int = Field(foreign_key="vendedores.vendedor_id")
