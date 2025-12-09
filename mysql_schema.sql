-- Fichier: mysql_schema.sql
SET FOREIGN_KEY_CHECKS = 0;

-- 1. Lieux
DROP TABLE IF EXISTS places;
CREATE TABLE places (
    id INT PRIMARY KEY AUTO_INCREMENT,
    region VARCHAR(100),
    province VARCHAR(100),
    city VARCHAR(100) NOT NULL,
    INDEX idx_city (city)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2. Hôpitaux
DROP TABLE IF EXISTS hospitals;
CREATE TABLE hospitals (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    place_id INT,
    address TEXT,
    type VARCHAR(100),
    beds INT DEFAULT 0,
    phone VARCHAR(100),
    email VARCHAR(150),
    website VARCHAR(255),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (place_id) REFERENCES places(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3. Fournisseurs (Dispositifs & Pharmaceutiques)
DROP TABLE IF EXISTS suppliers;
CREATE TABLE suppliers (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100), -- 'Dispositif Médical' ou 'Grossiste'
    activity TEXT,
    city VARCHAR(100),
    address TEXT,
    phone VARCHAR(100),
    responsible_pharmacist VARCHAR(150),
    INDEX idx_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4. Médicaments
DROP TABLE IF EXISTS medications;
CREATE TABLE medications (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL, -- SPECIALITE
    active_substance VARCHAR(255), -- SUBSTANCE ACTIVE
    dosage VARCHAR(100),
    form VARCHAR(100), -- FORME
    presentation VARCHAR(255),
    therapeutic_class VARCHAR(150),
    manufacturer VARCHAR(150), -- EPI
    price_public DECIMAL(10, 2), -- PPV
    price_hospital DECIMAL(10, 2), -- PH
    commercialization_status VARCHAR(100),
    INDEX idx_med_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 5. Équipements de référence
DROP TABLE IF EXISTS equipment;
CREATE TABLE equipment (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(150) NOT NULL,
    code VARCHAR(50),
    category VARCHAR(100),
    INDEX idx_eq_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 6. Services Médicaux
DROP TABLE IF EXISTS services;
CREATE TABLE services (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(150) NOT NULL,
    description TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- TABLES DE LIAISON

DROP TABLE IF EXISTS hospital_services;
CREATE TABLE hospital_services (
    hospital_id INT NOT NULL,
    service_id INT NOT NULL,
    PRIMARY KEY (hospital_id, service_id),
    FOREIGN KEY (hospital_id) REFERENCES hospitals(id) ON DELETE CASCADE,
    FOREIGN KEY (service_id) REFERENCES services(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS hospital_equipment;
CREATE TABLE hospital_equipment (
    hospital_id INT NOT NULL,
    equipment_id INT NOT NULL,
    quantity INT DEFAULT 1,
    PRIMARY KEY (hospital_id, equipment_id),
    FOREIGN KEY (hospital_id) REFERENCES hospitals(id) ON DELETE CASCADE,
    FOREIGN KEY (equipment_id) REFERENCES equipment(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;

-- Lier les médicaments aux hôpitaux (Inventaire)
CREATE TABLE hospital_medications (
    hospital_id INT NOT NULL,
    medication_id INT NOT NULL,
    stock_quantity INT DEFAULT 0,
    PRIMARY KEY (hospital_id, medication_id),
    FOREIGN KEY (hospital_id) REFERENCES hospitals(id) ON DELETE CASCADE,
    FOREIGN KEY (medication_id) REFERENCES medications(id) ON DELETE CASCADE
);

-- Lier les fournisseurs aux médicaments qu'ils vendent
CREATE TABLE supplier_medications (
    supplier_id INT NOT NULL,
    medication_id INT NOT NULL,
    PRIMARY KEY (supplier_id, medication_id),
    FOREIGN KEY (supplier_id) REFERENCES suppliers(id) ON DELETE CASCADE,
    FOREIGN KEY (medication_id) REFERENCES medications(id) ON DELETE CASCADE
);

-- Lier les fournisseurs aux équipements qu'ils distribuent
CREATE TABLE supplier_equipment (
    supplier_id INT NOT NULL,
    equipment_id INT NOT NULL,
    PRIMARY KEY (supplier_id, equipment_id),
    FOREIGN KEY (supplier_id) REFERENCES suppliers(id) ON DELETE CASCADE,
    FOREIGN KEY (equipment_id) REFERENCES equipment(id) ON DELETE CASCADE
);