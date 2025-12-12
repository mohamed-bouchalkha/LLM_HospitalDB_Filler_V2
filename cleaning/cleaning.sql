CREATE TABLE IF NOT EXISTS stg_places (
    raw_id INT AUTO_INCREMENT PRIMARY KEY,
    region VARCHAR(100),
    province VARCHAR(100),
    city VARCHAR(255),
    validation_status VARCHAR(50) DEFAULT 'PENDING'
);
DELIMITER //

CREATE PROCEDURE CleanPlaces()
BEGIN
    -- ==========================================================
    -- 1. REMOVE NOISE (Suffixes & Arabic)
    -- ==========================================================
    
    -- Remove administrative suffixes like (Mun.), (Arrond.) seen in Image 1
    UPDATE stg_places
    SET city = TRIM(REPLACE(REPLACE(city, '(Mun.)', ''), '(Arrond.)', ''))
    WHERE validation_status = 'PENDING';

    -- Remove Arabic text (Simple heuristic: keep text before the first Arabic char if possible, 
    -- or just split by space if the format is consistently "FrenchName ArabicName")
    -- Here we assume the format is "Name Name..." and we want to clean specific known patterns or non-latin chars
    -- For MySQL 8.0+ we could use REGEXP_REPLACE. For compatibility, we'll use a safer logic:
    
    -- Fix specific rows seen in your image (201, 203, 204, 215, 217)
    UPDATE stg_places SET city = 'Témara' WHERE city LIKE 'Témara%';
    UPDATE stg_places SET city = 'Ait Ouqabli' WHERE city LIKE 'Ait Ouqabli%';
    UPDATE stg_places SET city = 'Targuist' WHERE city LIKE 'Targuist%';
    UPDATE stg_places SET city = 'Ksar-el-Kebir' WHERE city LIKE 'Ksar-el-Kebir%';
    UPDATE stg_places SET city = 'Imouzzer Marmoucha' WHERE city LIKE 'Imouzzer Marmoucha%';
    UPDATE stg_places SET city = 'Bni Karrich' WHERE city LIKE 'Bni Karrich%';

    -- ==========================================================
    -- 2. FIX "ADDRESS AS CITY" (Row 212)
    -- ==========================================================
    
    -- Heuristic: If city contains "RUE" and "FES", it's Fès.
    UPDATE stg_places 
    SET city = 'Fès' 
    WHERE city LIKE '%RUE%' AND city LIKE '%FES%' AND validation_status = 'PENDING';

    -- General Case: If it starts with a number, it's likely an address. Flag it or try to extract.
    -- Here we try to extract common cities if they appear in the address string.
    UPDATE stg_places
    SET city = CASE 
        WHEN city LIKE '%CASABLANCA%' THEN 'Casablanca'
        WHEN city LIKE '%RABAT%' THEN 'Rabat'
        WHEN city LIKE '%FES%' OR city LIKE '%FÈS%' THEN 'Fès'
        WHEN city LIKE '%TANGER%' THEN 'Tanger'
        ELSE city 
    END
    WHERE (city REGEXP '^[0-9]+' OR city LIKE '%RUE%') -- Detects "2 RUE..."
    AND validation_status = 'PENDING';

    -- ==========================================================
    -- 3. STANDARDIZATION (Casing)
    -- ==========================================================

    -- Fix Uppercase names (TEMARA -> Temara, AOURIR -> Aourir)
    UPDATE stg_places
    SET city = CONCAT(UPPER(LEFT(city, 1)), LOWER(SUBSTRING(city, 2)))
    WHERE validation_status = 'PENDING';

    -- ==========================================================
    -- 4. FILL MISSING REGIONS (Self-Healing)
    -- ==========================================================

    -- If we have "Temara" with a region in row 1, but NULL in row 205, copy row 1's data.
    UPDATE stg_places t1
    JOIN stg_places t2 ON t1.city = t2.city
    SET t1.region = t2.region, t1.province = t2.province
    WHERE t1.region IS NULL AND t2.region IS NOT NULL
    AND t1.validation_status = 'PENDING';

    -- ==========================================================
    -- 5. FINAL VALIDATION
    -- ==========================================================
    
    -- Mark valid rows
    UPDATE stg_places SET validation_status = 'VALID' WHERE city IS NOT NULL;

END //

DELIMITER ;
INSERT INTO places (region, province, city)
SELECT DISTINCT 
    COALESCE(region, 'Region Inconnue'), -- Handle remaining NULLs
    COALESCE(province, 'Province Inconnue'), 
    city
FROM stg_places
WHERE validation_status = 'VALID'
GROUP BY city; -- Ensures we only get one row per city
-- ========================================================
-- PART 1: DATA CLEANING & STANDARDIZATION
-- ========================================================

-- 1. Standardize Text (Uppercase, Trim, Fix Spacing)
UPDATE medications
SET 
    name = UPPER(TRIM(name)),
    active_substance = UPPER(TRIM(active_substance)),
    form = UPPER(TRIM(form)),
    presentation = UPPER(TRIM(presentation)),
    therapeutic_class = UPPER(TRIM(therapeutic_class)),
    manufacturer = UPPER(TRIM(manufacturer)),
    commercialization_status = TRIM(commercialization_status);

-- 2. Clean Ingredient Separators
-- Changes "PARACETAMOL // CAFEINE" to "PARACETAMOL + CAFEINE"
UPDATE medications
SET active_substance = REGEXP_REPLACE(active_substance, '\\s*//\\s*', ' + ');

-- 3. Fix Dosage Spacing & Decimals
-- Adds space between numbers and letters (e.g., "500MG" -> "500 MG")
UPDATE medications
SET dosage = REGEXP_REPLACE(dosage, '([0-9])([a-zA-Z])', '$1 $2');

-- Fix Commas: Changes "0,4 ML" to "0.4 ML" (Standard SQL decimal format)
UPDATE medications
SET dosage = REPLACE(dosage, ',', '.')
WHERE dosage LIKE '%,%';

-- 4. Clean Numeric Prices
-- If 'price_public' was imported as text with commas (e.g. "3630,00"), fix it:
-- (Note: If your column is already FLOAT/DECIMAL, you can skip this)
-- UPDATE medications SET price_public = REPLACE(price_public, ',', '.') WHERE price_public LIKE '%,%';

-- 5. Handle "NULL" Strings
-- Converts the text string 'NULL' to a real SQL NULL value
UPDATE medications SET price_public = NULL WHERE price_public = 'NULL';
UPDATE medications SET price_hospital = NULL WHERE price_hospital = 'NULL';

-- ========================================================
-- PART 2: DATA VALIDATION
-- ========================================================

-- 6. Check for Logical Price Errors
-- Flags rows where the Hospital Price (usually lower) is higher than the Public Price
SELECT * FROM medications 
WHERE price_hospital > price_public;

-- 7. Check for Duplicates
-- Identifies drugs that look exactly the same
SELECT name, dosage, form, COUNT(*) as duplicate_count
FROM medications
GROUP BY name, dosage, form
HAVING COUNT(*) > 1;

-- 8. Check for Dosage Mismatches
-- Finds rows where you have 2 ingredients ("+") but only 1 dosage value (no "/")
SELECT name, active_substance, dosage 
FROM medications 
WHERE active_substance LIKE '%+%' 
  AND dosage NOT LIKE '%/%'
  AND dosage NOT LIKE '%+%';

-- 9. View Final Cleaned Data
SELECT * FROM medications LIMIT 50;
-- ========================================================
-- PART 0: SETUP ALGORITHMS & KNOWLEDGE BASE
-- ========================================================

-- 1. Create Levenshtein Function (Required for "Fuzzy Matching")
-- Calculates the edit distance between two strings (e.g., FEZ vs FES = 1).
DROP FUNCTION IF EXISTS LEVENSHTEIN;
DELIMITER $$
CREATE FUNCTION LEVENSHTEIN(s1 VARCHAR(255), s2 VARCHAR(255))
RETURNS INT
DETERMINISTIC
BEGIN
    DECLARE s1_len, s2_len, i, j, c, c_temp, cost INT;
    DECLARE s1_char CHAR;
    DECLARE cv0, cv1 VARBINARY(256);
    SET s1_len = CHAR_LENGTH(s1), s2_len = CHAR_LENGTH(s2), cv1 = 0x00, j = 1, i = 1, c = 0;
    IF s1 = s2 THEN RETURN 0;
    ELSEIF s1_len = 0 THEN RETURN s2_len;
    ELSEIF s2_len = 0 THEN RETURN s1_len;
    END IF;
    WHILE j <= s2_len DO SET cv1 = CONCAT(cv1, UNHEX(HEX(j))), j = j + 1; END WHILE;
    WHILE i <= s1_len DO
        SET s1_char = SUBSTRING(s1, i, 1), c = i, cv0 = UNHEX(HEX(i)), j = 1;
        WHILE j <= s2_len DO
            SET c = c + 1;
            IF s1_char = SUBSTRING(s2, j, 1) THEN SET cost = 0; ELSE SET cost = 1; END IF;
            SET c_temp = CONV(HEX(SUBSTRING(cv1, j, 1)), 16, 10) + cost;
            IF c > c_temp THEN SET c = c_temp; END IF;
            SET c_temp = CONV(HEX(SUBSTRING(cv1, j + 1, 1)), 16, 10) + 1;
            IF c > c_temp THEN SET c = c_temp; END IF;
            SET cv0 = CONCAT(cv0, UNHEX(HEX(c))), j = j + 1;
        END WHILE;
        SET cv1 = cv0, i = i + 1;
    END WHILE;
    RETURN c;
END$$
DELIMITER ;

-- 2. Create & Load the "Knowledge Base" (The Dictionary)
-- This allows the algorithms to fix specific missing data without hardcoding logic.
CREATE TEMPORARY TABLE IF NOT EXISTS city_dictionary (
    name_clean VARCHAR(100), 
    region VARCHAR(100), 
    province VARCHAR(100)
);
TRUNCATE TABLE city_dictionary; 

-- LOAD KNOWN CORRECT DATA (Add any new missing cities here)
INSERT INTO city_dictionary (name_clean, region, province) VALUES 
('MIDAR', 'Oriental', 'Driouch'),
('TIT MELLIL', 'Casablanca-Settat', 'Médiouna'),
('SIDI ALLAL EL BAHRAOUI', 'Rabat-Salé-Kénitra', 'Khemisset'),
('CHICHAOUA', 'Marrakech-Safi', 'Chichaoua'),
('AIT MELLOUL', 'Souss-Massa', 'Inezgane-Aït Melloul'),
('SALE', 'Rabat-Salé-Kénitra', 'Salé'),
('BENI ANSAR', 'Oriental', 'Nador'),
('TAOURIRT', 'Oriental', 'Taourirt'),
('MOHAMMEDIA', 'Casablanca-Settat', 'Mohammedia'),
('KENITRA', 'Rabat-Salé-Kénitra', 'Kénitra'),
('RABAT', 'Rabat-Salé-Kénitra', 'Rabat'),
('CASABLANCA', 'Casablanca-Settat', 'Casablanca'),
('KSAR EL KEBIR', 'Tanger-Tétouan-Al Hoceïma', 'Larache'),
('MARTIL', 'Tanger-Tétouan-Al Hoceïma', 'M\'diq-Fnideq'),
('SAADINA', 'Tanger-Tétouan-Al Hoceïma', 'Tétouan'),
('TOUISSIT', 'Oriental', 'Jerada'),
('TOULAL', 'Fès-Meknès', 'Meknès'),
('IMOUZZER MARMOUCHA', 'Fès-Meknès', 'Boulemane'),
('AOURIR', 'Souss-Massa', 'Agadir-Ida-Ou-Tanane'),
('SIDI IFNI', 'Guelmim-Oued Noun', 'Sidi Ifni'),
('TARGUIST', 'Tanger-Tétouan-Al Hoceïma', 'Al Hoceïma'),
('BEN AHMED', 'Casablanca-Settat', 'Settat'),
('BELYOUNECH', 'Tanger-Tétouan-Al Hoceïma', 'M\'diq-Fnideq'),
('CHEMAIA', 'Marrakech-Safi', 'Youssoufia');


-- ========================================================
-- PART 1: BASIC CLEANING
-- ========================================================

-- 3. Standardize Text (Uppercase, Trim, Remove Arabic/Symbols)
UPDATE places
SET city = UPPER(TRIM(REGEXP_REPLACE(city, '[^a-zA-Z0-9 ]', '')));

-- 4. Convert 'NULL' strings to real SQL NULLs
UPDATE places SET region = NULL WHERE region = 'NULL';
UPDATE places SET province = NULL WHERE province = 'NULL';

-- 5. Delete Obvious Junk (Streets starting with numbers)
DELETE FROM places 
WHERE city REGEXP '^[0-9]+' -- Starts with a number (e.g. "87 RUE...")
   OR city LIKE 'RUE %';


-- ========================================================
-- PART 2: THE "REPAIR" ALGORITHMS
-- ========================================================

-- 6. Algorithm A: The "Dictionary Matcher"
-- Matches broken rows to the dictionary. Handles exact matches and "spaceless" matches.
UPDATE places p
JOIN city_dictionary d 
    ON REPLACE(REPLACE(p.city, ' ', ''), '-', '') = REPLACE(REPLACE(d.name_clean, ' ', ''), '-', '')
SET 
    p.city = d.name_clean, -- Fixes spacing (e.g., KSARELKEBIR -> KSAR EL KEBIR)
    p.region = d.region,
    p.province = d.province
WHERE p.region IS NULL;

-- 7. Algorithm B: The "Phonetic Matcher" (Soundex)
-- Matches things that sound the same (e.g., SHISHAWA -> CHICHAOUA)
UPDATE places p
JOIN city_dictionary d ON SOUNDEX(p.city) = SOUNDEX(d.name_clean)
SET 
    p.city = d.name_clean,
    p.region = d.region,
    p.province = d.province
WHERE p.region IS NULL;

-- 8. Algorithm C: The "Typo Fixer" (Levenshtein)
-- Matches rows in the table to OTHER valid rows in the table that are 1-2 edits away.
UPDATE places p_bad
JOIN places p_good ON p_bad.id != p_good.id
SET 
    p_bad.city = p_good.city,
    p_bad.region = p_good.region,
    p_bad.province = p_good.province
WHERE 
    p_bad.region IS NULL 
    AND p_good.region IS NOT NULL
    AND LEVENSHTEIN(p_bad.city, p_good.city) BETWEEN 1 AND 2;


-- ========================================================
-- PART 3: THE "GARBAGE COLLECTOR" ALGORITHM
-- ========================================================

-- 9. Delete "Container" Rows (Neighborhoods like "AIN KADOUS FEZ")
-- Logic: If a broken row *contains* the name of a valid city (like "FES") 
-- but is longer/messier, we delete it because the valid city already exists.
DELETE p_garbage
FROM places p_garbage
JOIN places p_valid ON p_garbage.id != p_valid.id
WHERE 
    p_garbage.region IS NULL            -- The row is broken
    AND p_valid.region IS NOT NULL      -- The match is a valid city
    AND p_garbage.city LIKE CONCAT('%', p_valid.city, '%') -- Contains the valid name
    AND LENGTH(p_garbage.city) > LENGTH(p_valid.city);     -- Is messier/longer


-- ========================================================
-- PART 4: FINAL DEDUPLICATION
-- ========================================================

-- 10. Smart Deduplication
-- Deletes duplicates, but prioritizes keeping the row with Data (Region/Province).
DELETE p1
FROM places p1
INNER JOIN places p2 ON p1.city = p2.city
WHERE 
    p1.id != p2.id 
    AND (
        -- If p1 is empty and p2 is full, delete p1
        (p1.region IS NULL AND p2.region IS NOT NULL)
        OR 
        -- If both are equal quality, delete the newer duplicate (higher ID)
        ( (p1.region IS NULL) = (p2.region IS NULL) AND p1.id > p2.id )
    );


-- ========================================================
-- PART 5: VALIDATION
-- ========================================================

-- 11. Final Check
SELECT * FROM places ORDER BY city;

-- 12. Alert: Show any remaining stubborn NULLs
SELECT * FROM places WHERE region IS NULL;
-- ========================================================
-- PART 1: DATA CLEANING & STANDARDIZATION
-- ========================================================

-- 1. Standardize Text (Uppercase & Trim)
-- Ensures consistent casing for names and addresses.
UPDATE hospitals
SET 
    name = UPPER(TRIM(name)),
    address = UPPER(TRIM(address)),
    type = UPPER(TRIM(type)),
    email = LOWER(TRIM(email)),   -- Emails are standard lowercase
    website = LOWER(TRIM(website));

-- 2. Clean Addresses
-- Remove redundant "Maroc" or "Morocco" from the address string
UPDATE hospitals
SET address = TRIM(REPLACE(address, ' MAROC', ''));
UPDATE hospitals
SET address = TRIM(REPLACE(address, ', MOROCCO', ''));

-- Remove 5-digit Zip Codes embedded in the address (e.g., "12200 LARACHE")
-- Regex: Finds a 5-digit number followed by a space and removes it.
UPDATE hospitals
SET address = REGEXP_REPLACE(address, '[0-9]{5} ', '');

-- 3. Normalize Phone Numbers
-- Step A: Strip all characters that are NOT digits or '+'
UPDATE hospitals
SET phone = REGEXP_REPLACE(phone, '[^0-9+]', '')
WHERE phone IS NOT NULL;

-- Step B: Convert local '05'/'06' to International '+212'
-- Example: '0539861111' -> '+212539861111'
UPDATE hospitals
SET phone = CONCAT('+212', SUBSTRING(phone, 2))
WHERE phone LIKE '05%' OR phone LIKE '06%';

-- Step C: Fix cases where '+' is missing but starts with 212
UPDATE hospitals
SET phone = CONCAT('+', phone)
WHERE phone LIKE '212%';

-- 4. Fix "String NULLs"
-- Converts text string 'NULL' or empty strings to real SQL NULLs
UPDATE hospitals SET email = NULL WHERE email = 'NULL' OR email = '';
UPDATE hospitals SET website = NULL WHERE website = 'NULL' OR website = '';
UPDATE hospitals SET phone = NULL WHERE phone = 'NULL' OR phone = '';
UPDATE hospitals SET beds = NULL WHERE beds = 0; -- Optional: treat 0 beds as unknown/NULL


-- ========================================================
-- PART 2: DATA VALIDATION & CHECKS
-- ========================================================

-- 5. Check for Logical Duplicate Entries
-- Finds hospitals with the same name in the same place_id (City)
SELECT name, place_id, COUNT(*) as duplicate_count
FROM hospitals
GROUP BY name, place_id
HAVING COUNT(*) > 1;

-- 6. Validate "Type" Consistency
-- Lists all hospital types (HP, CHR, etc.) so you can spot typos (e.g., "H.P." vs "HP")
SELECT type, COUNT(*) as count 
FROM hospitals 
GROUP BY type
ORDER BY count DESC;

-- 7. Identify "Ghost" Hospitals (Missing Critical Info)
-- Flags hospitals that have NO phone and NO email (hard to contact)
SELECT * FROM hospitals 
WHERE phone IS NULL AND email IS NULL;

-- 8. Validate Email Format
-- Flags rows where email exists but looks wrong (missing '@' or '.')
SELECT id, name, email 
FROM hospitals 
WHERE email IS NOT NULL 
  AND email NOT REGEXP '^[A-Za-z0-9._%-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,4}$';

-- 9. View Final Cleaned Data
SELECT * FROM hospitals LIMIT 50;