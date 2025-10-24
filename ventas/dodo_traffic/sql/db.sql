DROP DATABASE oddo;

CREATE DATABASE oddo;

USE oddo;

CREATE TABLE
    vendedores (
        vendedor_id INT PRIMARY KEY AUTO_INCREMENT,
        nombre_completo VARCHAR(150),
        nombre VARCHAR(50)
    );

-- Tabla presupuestos
CREATE TABLE
    presupuestos (
        reserva_id INT PRIMARY KEY AUTO_INCREMENT,
        reserva CHAR(6),
        tipo_reserva CHAR(4),
        ultima_modif DATE,
        fecha_reserva DATE,
        fecha_salida DATE,
        cliente VARCHAR(255),
        nombre_grupo VARCHAR(255),
        estado VARCHAR(2),
        can_adu INT,
        can_chd INT,
        moneda VARCHAR(2),
        total DECIMAL(15, 2),
        costo_final DECIMAL(15, 2),
        ganancia DECIMAL(15, 2),
        productos VARCHAR(255),
        vendedor_id INT,
        FOREIGN KEY (vendedor_id) REFERENCES vendedores (vendedor_id)
    );

-- Tabla reservas
CREATE TABLE
    reservas (
        reserva_id INT PRIMARY KEY AUTO_INCREMENT,
        reserva CHAR(6),
        tipo_reserva CHAR(4),
        ultima_modif DATE,
        fecha_reserva DATE,
        fecha_salida DATE,
        fecha_fin DATE,
        cliente VARCHAR(255),
        nombre_grupo VARCHAR(255),
        estado VARCHAR(2),
        can_adu INTEGER,
        can_chd INTEGER,
        moneda VARCHAR(2),
        total DECIMAL(15, 2),
        ganancia DECIMAL(15, 2),
        eje_reservas_para VARCHAR(200),
        descrip_productos VARCHAR(255),
        vendedor_id INT,
        FOREIGN KEY (vendedor_id) REFERENCES vendedores (vendedor_id)
    );

-- Tabla oddo (CRM)
CREATE TABLE
    oddos (
        oddo_id INT PRIMARY KEY AUTO_INCREMENT,
        nombre_grupo VARCHAR(255),
        ganancia_esperada VARCHAR(100),
        estado VARCHAR(50),
        vendedor_id INT,
        FOREIGN KEY (vendedor_id) REFERENCES vendedores (vendedor_id)
    );

INSERT INTO
    vendedores (nombre_completo, nombre)
VALUES
    ('AGOSTINA MANCINELLI', 'AGOSTINA'),
    ('ANAHI DIAZ', 'ANAHI'),
    ('GABRIELA BRIZUELA', 'GABRIELA'),
    ('EMILCE ALONSO', 'EMILCE'),
    ('FLORENCIA BONIS', 'FLORENCIA'),
    ('LEANDRO ZAMPROGNO', 'LEANDRO'),
    ('CAROLINA LUDUENA', 'CAROLINA'),
    ('ANA MAIBACH', 'ANA'),
    ('VALENTINA NICOLAI', 'VALENTINA'),
    ('CATALINA HAK', 'CATALINA'),
    ('LEILA CAMINOS', 'LEILA'),
    ('CHAGRA MARTIN', 'CHAGRA'),
    ('LAURA NICOLAI', 'LAURA'),
    ('DAIANA DEGELE', 'DAIANA'),
    ('CINTHYA OVIEDO', 'CINTHYA'),
    ('VERONICA BRAVO', 'VERONICA'),
    ('SOFIA NAVARRO', 'SOFIA'),
    ('ROXANA ORINGO', 'ROXANA'),
    ('CLEMENTINA BORTOLETTO', 'CLEMENTINA'),
    ('MARIA PAULA SORIA', 'MARIA PAULA'),
    ('JULIAN RODRIGUEZ', 'JULIAN'),
    ('TEST TEST', 'TEST'),
    ('CELESTE PALADINI', 'CELESTE'),
    ('MATEO CACERES', 'MATEO'),
    ('PAOLA BRAVO', 'PAOLA'),
    ('CAMILA FILIPINI', 'CAMILA'),
    ('GUADALUPE CORVALAN', 'GUADALUPE'),
    ('DANIELA PEREZ', 'DANIELA'),
    ('DIEGO GUILLERMO PELOSO', "DIEGO GUILLERMO"),
    ('REBECA ANDRADE', 'REBECA'),
    ('LETICIA MINÉ', 'LETICIA');

CREATE VIEW
    vista_reservas_con_presupuestos AS
SELECT
    -- Datos de la reserva (tabla principal)
    r.reserva,
    r.tipo_reserva,
    r.ultima_modif,
    r.fecha_reserva,
    r.fecha_salida,
    r.fecha_fin,
    r.cliente,
    r.nombre_grupo,
    r.estado AS estado_reserva,
    r.can_adu,
    r.can_chd,
    r.moneda,
    r.total AS total_reserva,
    r.ganancia AS ganancia_reserva,
    r.eje_reservas_para,
    r.descrip_productos,
    v.nombre_completo AS nombre_vendedor,
    -- Datos del presupuesto relacionado (si existe)
    p.total AS total_presupuesto,
    p.costo_final AS costo_presupuesto,
    p.ganancia AS ganancia_presupuesto,
    p.productos AS productos_presupuesto,
    p.fecha_reserva AS fecha_reserva_presupuesto,
    -- Indicador de si tiene presupuesto asociado
    CASE
        WHEN p.reserva IS NOT NULL THEN 'SÍ'
        ELSE 'NO'
    END AS tiene_presupuesto
FROM
    reservas r
    LEFT JOIN presupuestos p ON r.reserva = p.reserva
    LEFT JOIN vendedores v ON r.vendedor_id = v.vendedor_id;