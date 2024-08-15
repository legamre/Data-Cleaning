SELECT * 
FROM layoffs;

CREATE TABLE layoffs_staging 
LIKE layoffs;

SELECT * 
FROM layoffs_staging

INSERT INTO layoffs_staging 
SELECT * 
FROM layoffs.layoffs;

DESCRIBE layoffs_staging;

SELECT company, industry, total_laid_off,`date`, ROW_NUMBER() OVER (PARTITION BY company, industry, total_laid_off,`date`) AS row_num
FROM layoffs.layoffs_staging;

-- Finding duplicates
SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`, ROW_NUMBER() OVER (PARTITION BY company, industry, total_laid_off,`date`) AS row_num
	FROM layoffs.layoffs_staging
) duplicates
WHERE row_num > 1;

-- Final code for finding duplicates
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM layoffs.layoffs_staging
) duplicates
WHERE row_num > 1;


SET SQL_SAFE_UPDATES = 0;


-- Deleting duplicates
WITH DELETE_CTE AS (
    SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, 
        ROW_NUMBER() OVER (
            PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
        ) AS row_num
    FROM layoffs.layoffs_staging
)
DELETE FROM layoffs.layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) IN (
    SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    FROM DELETE_CTE
    WHERE row_num > 1
);


ALTER TABLE layoffs.layoffs_staging ADD row_num INT;


SELECT *
FROM layoffs.layoffs_staging

-- Alternatively
-- Creating a new table
CREATE TABLE `layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO `layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
            ) AS row_num
	FROM layoffs.layoffs_staging;

-- Deleting rows were row_num is greater than 2
DELETE FROM layoffs.layoffs_staging2
WHERE row_num >= 2;

-- Standardizing Data
SELECT * 
FROM layoffs.layoffs_staging2;

-- Finding null and empty rows
SELECT DISTINCT industry
FROM layoffs.layoffs_staging2
ORDER BY industry;

SELECT *
FROM layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SELECT *
FROM layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT *
FROM layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';

-- Updating any other row with the same company name to the non-null industry values
UPDATE layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;


-- Standarizing Crypto variations
SELECT DISTINCT industry
FROM layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

SELECT DISTINCT industry
FROM layoffs.layoffs_staging2
ORDER BY industry;

SELECT *
FROM layoffs.layoffs_staging2;

-- Standarizing United States variations
SELECT DISTINCT country
FROM layoffs.layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

SELECT DISTINCT country
FROM layoffs.layoffs_staging2
ORDER BY country;


-- Fixing the date columns:
SELECT *
FROM layoffs.layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs.layoffs_staging2;

-- Deleting useless data 
SELECT *
FROM layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT *
FROM layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE FROM layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM layoffs.layoffs_staging2;
