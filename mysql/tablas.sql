CREATE TABLE dev.m_productos(
ID VARCHAR(99),
nombre VARCHAR(99)
);

-- t_metadata
CREATE TABLE IF NOT EXISTS dev.t_metadata (
  id INT NOT NULL AUTO_INCREMENT,
  source JSON NOT NULL,
  type VARCHAR(32) NOT NULL,
  target JSON NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE dev.t_metadata ADD COLUMN INGESTION_NAME VARCHAR(99);

-- tiempos_produccion
CREATE TABLE dev.m_tiempos_produccion (
  cod_producto VARCHAR(16) NOT NULL,
  id INT NOT NULL,
  tiempo_produccion DOUBLE NOT NULL,
  PRIMARY KEY (cod_producto, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

