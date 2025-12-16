INSERT INTO dev.m_productos VALUES ( 'P001', 'Funko Pop');
INSERT INTO dev.m_productos VALUES ('P002', 'Memoria RAM DDR5 32 GB');
INSERT INTO dev.m_productos VALUES ('P003', 'Motor de Coche');
INSERT INTO dev.m_productos VALUES ('P004', 'Bolígrafo');

-- t_metadata
TRUNCATE TABLE dev.t_metadata;
INSERT INTO dev.t_metadata (source, type, target, active)
SELECT
  JSON_OBJECT('db', t.TABLE_SCHEMA, 'table', t.TABLE_NAME) AS source,
  'table' AS type,
  JSON_OBJECT('db', 'dev', 'table', 'cdp') AS target,
  TRUE AS active
FROM information_schema.TABLES t
WHERE 1=1
  AND t.TABLE_SCHEMA = 'dev'
  AND t.TABLE_NAME LIKE 'p%';

INSERT INTO dev.m_tiempos_produccion (cod_producto, id, tiempo_produccion) VALUES
-- P001 (Funko Pop) -> 6 ids
('P001', 1, 0.0015),
('P001', 2, 0.0010),
('P001', 3, 0.0008),
('P001', 4, 0.0006),
('P001', 5, 0.0005),
('P001', 6, 0.0020),

-- P002 (Memoria RAM DDR5 32 GB) -> 7 ids
('P002', 1, 0.0800),
('P002', 2, 0.0500),
('P002', 3, 0.0300),
('P002', 4, 0.0400),
('P002', 5, 0.0200),
('P002', 6, 0.0600),
('P002', 7, 0.0100),

-- P003 (Motor de coche) -> 8 ids
('P003', 1, 0.1500),
('P003', 2, 0.1200),
('P003', 3, 0.1000),
('P003', 4, 0.0800),
('P003', 5, 0.0500),
('P003', 6, 0.0900),
('P003', 7, 0.0400),
('P003', 8, 0.0300),

-- P004 (Bolígrafo) -> 6 ids
('P004', 1, 0.00005),
('P004', 2, 0.00004),
('P004', 3, 0.00003),
('P004', 4, 0.00002),
('P004', 5, 0.00002),
('P004', 6, 0.00006);
